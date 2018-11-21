/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hawkular.openshift.auth;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.openshift.auth.Utils.endExchange;

import static io.undertow.util.Headers.ACCEPT;
import static io.undertow.util.Headers.AUTHORIZATION;
import static io.undertow.util.Headers.CONTENT_LENGTH;
import static io.undertow.util.Headers.CONTENT_TYPE;
import static io.undertow.util.Headers.HOST;
import static io.undertow.util.Methods.DELETE;
import static io.undertow.util.Methods.GET;
import static io.undertow.util.Methods.POST;
import static io.undertow.util.Methods.PUT;
import static io.undertow.util.StatusCodes.BAD_REQUEST;
import static io.undertow.util.StatusCodes.CREATED;
import static io.undertow.util.StatusCodes.FORBIDDEN;
import static io.undertow.util.StatusCodes.INTERNAL_SERVER_ERROR;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.hawkular.metrics.api.jaxrs.util.MetricRegistryProvider;
import org.hawkular.metrics.core.dropwizard.HawkularMetricRegistry;
import org.jboss.logging.Logger;
import org.xnio.BufferAllocator;
import org.xnio.ByteBufferSlicePool;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.Xnio;
import org.xnio.XnioExecutor;
import org.xnio.XnioIoThread;
import org.xnio.ssl.XnioSsl;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Uninterruptibles;

import io.undertow.Undertow;
import io.undertow.client.ClientCallback;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientExchange;
import io.undertow.client.ClientRequest;
import io.undertow.client.UndertowClient;
import io.undertow.connector.ByteBufferPool;
import io.undertow.protocols.ssl.UndertowXnioSsl;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.XnioByteBufferPool;
import io.undertow.util.AttachmentKey;
import io.undertow.util.HttpString;
import io.undertow.util.StatusCodes;
import io.undertow.util.StringReadChannelListener;
import io.undertow.util.StringWriteChannelListener;

/**
 * An authentication/authorization strategy which relies on Openshift's OAuth server, using a non-blocking HTTP client.
 *
 * @author Thomas Segismont
 */
class TokenAuthenticator implements Authenticator {
    private static final Logger log = Logger.getLogger(TokenAuthenticator.class);

    private static final AttachmentKey<AuthContext> AUTH_CONTEXT_KEY = AttachmentKey.create(AuthContext.class);

    private static final HttpString HAWKULAR_TENANT = new HttpString("Hawkular-Tenant");
    static final String BEARER_PREFIX = "Bearer ";
    private static final String MISSING_HEADERS_MSG =
            "The '" + AUTHORIZATION + "' and '" + HAWKULAR_TENANT + "' headers are required";
    private static final String UNAUTHORIZED_USER_EDIT_MSG =
            "Users are not authorized to perform edits on metric data";

    //The resource to check against for security purposes. For this version we are allowing Metrics based on a users
    //access to the pods in in a particular project.
    private static final String RESOURCE = "pods";
    private static final String KIND = "SubjectAccessReview";

    private static final Map<HttpString, String> VERBS;
    private static final String VERBS_DEFAULT;

    static {
        Map<HttpString, String> verbs = new HashMap<>();
        verbs.put(GET, "list");
        verbs.put(PUT, "update");
        verbs.put(POST, "update");
        verbs.put(DELETE, "update");
        verbs.put(new HttpString("PATCH"), "update");
        VERBS = Collections.unmodifiableMap(verbs);
        // default to 'list' verb (which is the lowest level permission)
        VERBS_DEFAULT = VERBS.get(GET);
    }

    private static final String KUBERNETES_MASTER_URL_SYSPROP = "KUBERNETES_MASTER_URL";
    private static final String USER_WRITE_ACCESS_SYSPROP = "USER_WRITE_ACCESS";
    private static final String KUBERNETES_MASTER_URL_DEFAULT = "https://kubernetes.default.svc.cluster.local";
    private static final String KUBERNETES_MASTER_URL =
            System.getProperty(KUBERNETES_MASTER_URL_SYSPROP, KUBERNETES_MASTER_URL_DEFAULT);
    private static final String USER_WRITE_ACCESS = System.getProperty(USER_WRITE_ACCESS_SYSPROP, "false");
    private static final String ACCESS_URI = "/oapi/v1/subjectaccessreviews";
    private static final int MAX_CONNECTIONS_PER_THREAD = 20;
    private static final long CONNECTION_WAIT_TIMEOUT = MILLISECONDS.convert(30, SECONDS);
    private static final String TIMEDOUT_WAITING_CONNECTION = "Could not acquire a Kubernetes client connection";
    private static final long CONNECTION_TTL = MILLISECONDS.convert(10, SECONDS);
    private static final int MAX_RETRY = 5;
    private static final int MAX_PENDING = 32 * 1024;
    private static final String TOO_MANY_PENDING_REQUESTS = "Too many pending requests";
    private static final String CLIENT_REQUEST_FAILURE = "Kubernetes client request failure";

    private static final String METRICS_SCOPE = "OpenShift";
    private static final String METRICS_TYPE = "Security";


    private final HttpHandler containerHandler;
    private final ObjectMapper objectMapper;
    private final URI kubernetesMasterUri;
    private final ConcurrentMap<XnioIoThread, ConnectionPool> connectionPools;
    private final ConnectionFactory connectionFactory;
    private final Timer authLatency;
    private final Timer apiLatency;

    private final Pattern postQuery;
    private final String resourceName;
    private final String componentName;

    TokenAuthenticator(HttpHandler containerHandler, String componentName, String resourceName, Pattern postQuery) {
        this.containerHandler = containerHandler;
        this.resourceName = resourceName;
        this.componentName = componentName;

        this.postQuery = postQuery;
        objectMapper = new ObjectMapper();
        try {
            kubernetesMasterUri = new URI(KUBERNETES_MASTER_URL);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        connectionPools = new ConcurrentHashMap<>(Runtime.getRuntime().availableProcessors(), 1);
        connectionFactory = new ConnectionFactory(kubernetesMasterUri);
        HawkularMetricRegistry metrics = MetricRegistryProvider.INSTANCE.getMetricRegistry();
        // Note: HawkularMetricRegistry.registerMetaData cannot be used here because the registry is not yet
        // fully initialized. Calling registerMetaData will result in an NPE.
        authLatency = metrics.timer("openshift-oauth-latency");
        apiLatency = metrics.timer("openshift-oauth-kubernetes-response-time");
    }

    @Override
    public void handleRequest(HttpServerExchange serverExchange) throws Exception {
        AuthContext context = AuthContext.initialize(serverExchange);
        serverExchange.putAttachment(AUTH_CONTEXT_KEY, context);
        // Make sure the exchange attachment is removed in the end
        serverExchange.addExchangeCompleteListener((exchange, nextListener) -> {
            exchange.removeAttachment(AUTH_CONTEXT_KEY);
            nextListener.proceed();
        });
        if (context.isMissingTenantHeader()) {
            endExchange(serverExchange, BAD_REQUEST, MISSING_HEADERS_MSG);
            return;
        }

        // Marks the request as dispatched. If we don't do this, the exchange will be terminated by the container when
        // this method returns, but we need to wait for Kubernetes' master response.
        serverExchange.dispatch();
        XnioIoThread ioThread = serverExchange.getIoThread();
        ConnectionPool connectionPool = connectionPools.computeIfAbsent(ioThread, t -> new ConnectionPool(connectionFactory, componentName));
        PooledConnectionWaiter waiter = createWaiter(serverExchange);
        if (!connectionPool.offer(waiter)) {
            endExchange(serverExchange, INTERNAL_SERVER_ERROR, TOO_MANY_PENDING_REQUESTS);
        }
    }

    //Returns if the request is a query request, eg to perform a READ
    private boolean isQuery(HttpServerExchange serverExchange) {
        if (serverExchange.getRequestMethod().toString().equalsIgnoreCase("GET") || serverExchange.getRequestMethod().toString().equalsIgnoreCase("HEAD")) {
            // all GET requests are considered queries
            return true;
        } else if (serverExchange.getRequestMethod().toString().equalsIgnoreCase("POST")) {
            // some POST requests may be queries we need to check.
            if (postQuery != null && postQuery.matcher(serverExchange.getRelativePath()).find()) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private PooledConnectionWaiter createWaiter(HttpServerExchange serverExchange) {
        Consumer<PooledConnection> onGet = connection -> sendAuthenticationRequest(serverExchange, connection);
        Runnable onTimeout = () -> onPooledConnectionWaitTimeout(serverExchange);
        return new PooledConnectionWaiter(onGet, onTimeout);
    }

    /**
     * Executed when a pooled connection is acquired.
     */
    private void sendAuthenticationRequest(HttpServerExchange serverExchange, PooledConnection connection) {
        AuthContext context = serverExchange.getAttachment(AUTH_CONTEXT_KEY);
        String verb = getVerb(serverExchange);

        String resource;
        // if we are not dealing with a query
        if (!isQuery(serverExchange)) {
            // is USER_WRITE_ACCESS is disabled, then use the legacy check.
            // Otherwise check using the actual resource (eg 'hawkular-metrics', 'hawkular-alerts', etc)
            if (USER_WRITE_ACCESS.equalsIgnoreCase("true")) {
                resource = RESOURCE;
            } else {
                resource= resourceName;
            }
        } else {
            resource = RESOURCE;
        }

        context.subjectAccessReview = generateSubjectAccessReview(context.tenant, verb, resource);
        ClientRequest request = buildClientRequest(context);
        context.clientRequestStarting();
        connection.sendRequest(request, new RequestReadyCallback(serverExchange, connection));
    }

    /**
     * Executed if no poooled connection was made available in a timely manner.
     */
    private void onPooledConnectionWaitTimeout(HttpServerExchange serverExchange) {
        endExchange(serverExchange, INTERNAL_SERVER_ERROR, TIMEDOUT_WAITING_CONNECTION);
    }

    /**
     * Determine the verb we should apply based on the HTTP method being requested.
     *
     * @return the verb to use
     */
    private String getVerb(HttpServerExchange serverExchange) {
        // if its a query type verb, then treat as a GET type call.
        if (isQuery(serverExchange)) {
            return VERBS.get(GET);
        } else {
            String verb = VERBS.get(serverExchange.getRequestMethod());
            if (verb == null) {
                log.debugf("Unhandled http method '%s'. Checking for read access.", serverExchange.getRequestMethod());
                verb = VERBS_DEFAULT;
            }
            return verb;
        }
    }

    /**
     * Generates a SubjectAccessReview object used to request if a user has a certain permission or not.
     *
     * @param namespace the namespace
     * @param verb      the requested permission
     *
     * @return JSON text representation of the SubjectAccessReview object
     */
    private String generateSubjectAccessReview(String namespace, String verb, String resource) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("apiVersion", "v1");
        objectNode.put("kind", KIND);
        objectNode.put("resource", resource);
        objectNode.put("verb", verb);
        objectNode.put("namespace", namespace);
        return objectNode.toString();
    }

    private ClientRequest buildClientRequest(AuthContext context) {
        ClientRequest request = new ClientRequest().setMethod(POST).setPath(ACCESS_URI);
        String host = kubernetesMasterUri.getHost();
        int port = kubernetesMasterUri.getPort();
        String hostHeader = (port == -1) ? host : (host + ":" + port);
        request.getRequestHeaders()
                .add(HOST, hostHeader)
                .add(ACCEPT, "application/json")
                .add(CONTENT_TYPE, "application/json")
                .add(AUTHORIZATION, context.authorizationHeader)
                .add(CONTENT_LENGTH, context.subjectAccessReview.length());
        return request;
    }

    /**
     * Called when the Kubernetes master server reponse has been inspected.
     */
    private void onRequestResult(HttpServerExchange serverExchange, PooledConnection connection, boolean allowed) {
        connectionPools.get(serverExchange.getIoThread()).release(connection);
        // Remove attachment early to make it eligible for GC
        AuthContext context = serverExchange.removeAttachment(AUTH_CONTEXT_KEY);
        apiLatency.update(context.getClientResponseTime(), NANOSECONDS);
        authLatency.update(context.getLatency(), NANOSECONDS);
        if (allowed) {
            serverExchange.dispatch(containerHandler);
        } else {
            endExchange(serverExchange, FORBIDDEN);
        }
    }

    /**
     * Called if an exception occurs at any stage in the process.
     */
    private void onRequestFailure(HttpServerExchange serverExchange, PooledConnection connection, IOException e,
                                  boolean retry) {
        log.debug("Client request failure", e);
        IoUtils.safeClose(connection);
        ConnectionPool connectionPool = connectionPools.get(serverExchange.getIoThread());
        connectionPool.release(connection);
        AuthContext context = serverExchange.getAttachment(AUTH_CONTEXT_KEY);
        if (context.retries < MAX_RETRY && retry) {
            context.retries++;
            PooledConnectionWaiter waiter = createWaiter(serverExchange);
            if (!connectionPool.offer(waiter)) {
                endExchange(serverExchange, INTERNAL_SERVER_ERROR, TOO_MANY_PENDING_REQUESTS);
            }
        } else {
            endExchange(serverExchange, INTERNAL_SERVER_ERROR, CLIENT_REQUEST_FAILURE);
        }
    }

    @Override
    public void stop() {
        Set<Entry<XnioIoThread, ConnectionPool>> entries = connectionPools.entrySet();
        CountDownLatch latch = new CountDownLatch(entries.size());
        entries.forEach(entry -> {
            // Connection pool is not thread safe and #stop must be called on the corresponding io thread
            entry.getKey().execute(() -> entry.getValue().stop(latch::countDown));
        });
        Uninterruptibles.awaitUninterruptibly(latch, 5, SECONDS);
        connectionFactory.close();
    }

    /**
     * Contextual data needed to perform the authentication process. An instance of this class is attached to the
     * {@link HttpServerExchange}. That makes it easy to retrieve information without passing a lot of arguments
     * around.
     */
    private static final class AuthContext {
        private long creation;
        private String authorizationHeader;
        private String tenant;
        private String subjectAccessReview;
        private int retries;
        private long requestStart;
        private long requestStop;

        private static AuthContext initialize(HttpServerExchange serverExchange) {
            AuthContext context = new AuthContext();
            context.creation = System.nanoTime();
            context.authorizationHeader = serverExchange.getRequestHeaders().getFirst(AUTHORIZATION);
            context.tenant = serverExchange.getRequestHeaders().getFirst(HAWKULAR_TENANT);
            return context;
        }

        private boolean isMissingTenantHeader() {
            return tenant == null;
        }

        private void clientRequestStarting() {
            requestStart = System.nanoTime();
        }

        private void clientResponseReceived() {
            requestStop = System.nanoTime();
        }

        private long getClientResponseTime() {
            return requestStop - requestStart;
        }

        private long getLatency() {
            return requestStop - creation;
        }
    }

    /**
     * Callback invoked when the client exchange is ready for sending data.
     */
    private class RequestReadyCallback implements ClientCallback<ClientExchange> {
        private final HttpServerExchange serverExchange;
        private final PooledConnection connection;

        private RequestReadyCallback(HttpServerExchange serverExchange, PooledConnection connection) {
            this.serverExchange = serverExchange;
            this.connection = connection;
        }

        @Override
        public void completed(ClientExchange clientExchange) {
            clientExchange.setResponseListener(new ResponseListener(serverExchange, connection));
            writeBody(clientExchange);
        }

        private void writeBody(ClientExchange clientExchange) {
            AuthContext context = serverExchange.getAttachment(AUTH_CONTEXT_KEY);
            StringWriteChannelListener writeChannelListener;
            writeChannelListener = new StringWriteChannelListener(context.subjectAccessReview);
            writeChannelListener.setup(clientExchange.getRequestChannel());
        }

        @Override
        public void failed(IOException e) {
            onRequestFailure(serverExchange, connection, e, true);
        }
    }

    /**
     * Callback invoked when the server replied.
     */
    private class ResponseListener implements ClientCallback<ClientExchange> {
        private final HttpServerExchange serverExchange;
        private final PooledConnection connection;

        private ResponseListener(HttpServerExchange serverExchange, PooledConnection connection) {
            this.serverExchange = serverExchange;
            this.connection = connection;
        }

        @Override
        public void completed(ClientExchange clientExchange) {
            StringReadChannelListener readChannelListener;
            readChannelListener = new ResponseBodyListener(serverExchange, connection, clientExchange);
            readChannelListener.setup(clientExchange.getResponseChannel());
        }

        @Override
        public void failed(IOException e) {
            onRequestFailure(serverExchange, connection, e, true);
        }
    }

    /**
     * Callback invoked when the server reponse body has been fully read.
     */
    private class ResponseBodyListener extends StringReadChannelListener {
        private final HttpServerExchange serverExchange;
        private final PooledConnection connection;
        private final ClientExchange clientExchange;

        private ResponseBodyListener(HttpServerExchange serverExchange, PooledConnection connection,
                ClientExchange clientExchange) {
            super(clientExchange.getConnection().getBufferPool());
            this.serverExchange = serverExchange;
            this.connection = connection;
            this.clientExchange = clientExchange;
        }

        @Override
        protected void stringDone(String body) {
            AuthContext context = serverExchange.getAttachment(AUTH_CONTEXT_KEY);
            context.clientResponseReceived();
            int responseCode = clientExchange.getResponse().getResponseCode();
            if (responseCode == CREATED) {
                try {
                    JsonNode jsonNode = objectMapper.readTree(body);
                    JsonNode allowedNode = jsonNode == null ? null : jsonNode.get("allowed");
                    boolean allowed = allowedNode != null && allowedNode.asBoolean();
                    onRequestResult(serverExchange, connection, allowed);
                } catch (IOException e) {
                    onRequestFailure(serverExchange, connection, e, true);
                }
            } else {
                IOException e = new IOException(StatusCodes.getReason(responseCode));
                if(responseCode >= 500) {
                    onRequestFailure(serverExchange, connection, e, true);
                } else {
                    // Retries will not help to solve the issue - client issue
                    onRequestFailure(serverExchange, connection, e, false);
                }
            }
        }

        @Override
        protected void error(IOException e) {
            onRequestFailure(serverExchange, connection, e, true);
        }
    }

    /**
     * A {@link ClientConnection} pool. Each {@link XnioIoThread} has its own pool. While it may not be perfect if
     * the container does not evenly assign requests to IO threads, implementation is easier as no synchronization is
     * needed. But remember that only the corresponding io thread must manipulate its own pool instance.
     *
     * @author snegrea
     */
    private static class ConnectionPool {
        private final ConnectionFactory connectionFactory;
        private final List<PooledConnection> connections;
        private final Queue<PooledConnectionWaiter> waiters;
        private final XnioExecutor.Key periodicTaskKey;
        private int ongoingCreations;
        private boolean stop;
        private volatile int connectionCount;
        private volatile int waiterCount;

        private ConnectionPool(ConnectionFactory connectionFactory, String componentName) {
            this.connectionFactory = connectionFactory;
            connections = new ArrayList<>(MAX_CONNECTIONS_PER_THREAD);
            waiters = new ArrayDeque<>();
            XnioIoThread ioThread = (XnioIoThread) Thread.currentThread();
            periodicTaskKey = ioThread.executeAtInterval(this::periodicTask, 1, SECONDS);
            ongoingCreations = 0;
            stop = false;
        }

        /**
         * This task is executed periodically to make sure no waiter stay in the queue longer than needed and no stale
         * connection occupies the pool.
         */
        private void periodicTask() {
            if (stop) {
                return;
            }
            // Close stale connections and remove them from the pool
            long now = System.currentTimeMillis();
            for (Iterator<PooledConnection> iterator = connections.iterator(); iterator.hasNext(); ) {
                PooledConnection connection = iterator.next();
                if (connection.idle && !connection.canReuse(now)) {
                    iterator.remove();
                    IoUtils.safeClose(connection);
                }
            }
            removeTimedOutWaiters();
            // Create a connection if pool is not full and some clients are waiting
            if (!waiters.isEmpty() && !isFull()) {
                createConnection();
            }
            // Side job, update metrics
            connectionCount = connections.size();
            waiterCount = waiters.size();
        }

        /**
         * @return false if no connection is available immediately and too many clients are waiting, true otherwise
         */
        private boolean offer(PooledConnectionWaiter waiter) {
            if (stop) {
                waiter.onTimeout.run();
                return true;
            }
            removeTimedOutWaiters();
            // We must queue the waiter and not take an available connection immediatly, in order to preserve to the
            // FIFO requirement for clients
            if (waiters.size() < MAX_PENDING) {
                waiters.offer(waiter);
            } else {
                return false;
            }
            // Now try to acquire a connection
            PooledConnection selected = selectIdleConnection();
            if (selected != null) {
                // Got one, poll a waiter (may be the caller or a previous one)
                waiters.poll().onGet.accept(selected);
            }
            return true;
        }

        private PooledConnection selectIdleConnection() {
            // For maximum speed we iterate through the list until we find a reusable connection
            // It's not necessary to close all stale connections here, the periodic task will take care of this
            long now = System.currentTimeMillis();
            for (Iterator<PooledConnection> iterator = connections.iterator(); iterator.hasNext(); ) {
                PooledConnection connection = iterator.next();
                if (connection.idle) {
                    if (connection.canReuse(now)) {
                        connection.idle = false;
                        return connection;
                    } else {
                        iterator.remove();
                        IoUtils.safeClose(connection);
                    }
                }
            }
            return null;
        }

        private void release(PooledConnection connection) {
            connection.idle = true;
            if (stop) {
                return;
            }
            // Don't keep the connection in the pool if we can't reuse it
            if (!connection.canReuse(System.currentTimeMillis())) {
                connections.remove(connection);
                IoUtils.safeClose(connection);
            }
            removeTimedOutWaiters();
            // Stop here if no client is waiting for a connection
            if (!waiters.isEmpty()) {
                PooledConnection selected = selectIdleConnection();
                if (selected != null) {
                    // Got a connection, can poll a waiter now
                    PooledConnectionWaiter waiter = waiters.poll();
                    waiter.onGet.accept(selected);
                } else if (!isFull()) {
                    createConnection();
                }
            }
        }

        private void removeTimedOutWaiters() {
            long now = System.currentTimeMillis();
            for (Iterator<PooledConnectionWaiter> iterator = waiters.iterator(); iterator.hasNext(); ) {
                PooledConnectionWaiter waiter = iterator.next();
                if (waiter.timestamp + CONNECTION_WAIT_TIMEOUT < now) {
                    iterator.remove();
                    waiter.onTimeout.run();
                } else {
                    // waiters is a FIFO queue, so we can stop as soon as we find one which is not timed out
                    break;
                }
            }
        }

        private boolean isFull() {
            return connections.size() + ongoingCreations == MAX_CONNECTIONS_PER_THREAD;
        }

        private void createConnection() {
            ongoingCreations++;
            try {
                connectionFactory.createConnection(new ClientCallback<ClientConnection>() {
                    @Override
                    public void completed(ClientConnection result) {
                        ongoingCreations--;
                        onConnectionCreated(result);
                    }

                    @Override
                    public void failed(IOException e) {
                        ongoingCreations--;
                        onConnectionCreationFailure(e);
                    }
                });
            } catch (UnresolvedAddressException e) {
                ongoingCreations--;
                onConnectionCreationFailure(e);
            }
        }

        private void onConnectionCreated(ClientConnection clientConnection) {
            if (stop) {
                IoUtils.safeClose(clientConnection);
                return;
            }
            PooledConnection connection = new PooledConnection();
            connection.clientConnection = clientConnection;
            connections.add(connection);
            removeTimedOutWaiters();
            if (!waiters.isEmpty()) {
                // If there are waiters, pick up one.
                PooledConnectionWaiter waiter = waiters.poll();
                connection.idle = false;
                waiter.onGet.accept(connection);
            } else {
                // Otherwise make the connection available for future clients
                connection.idle = true;
            }
        }

        private void onConnectionCreationFailure(Exception e) {
            log.debug("Failed to create client connection", e);
            if (stop) {
                return;
            }
            // Wait a bit before trying to create a connection again
            XnioIoThread ioThread = (XnioIoThread) Thread.currentThread();
            ioThread.executeAfter(() -> {
                removeTimedOutWaiters();
                if (!stop && !waiters.isEmpty() && !isFull()) {
                    // It's still necessary to create a connection only if the pool is not stopped, there still is
                    // a client waiting for a connection and the pool is not full
                    createConnection();
                }
            }, 1, SECONDS);
        }

        /**
         * @param onStop callback invoked when waiters have all been notified and all client connections are closed
         */
        private void stop(Runnable onStop) {
            stop = true;
            // Cancel the periodic task
            periodicTaskKey.remove();
            // Simple strategy, tell the waiters they failed to acquire a connection
            while (!waiters.isEmpty()) {
                PooledConnectionWaiter waiter = waiters.poll();
                waiter.onTimeout.run();
            }
            closeAllConnections(onStop);
        }

        private void closeAllConnections(Runnable onAllClosed) {
            for (Iterator<PooledConnection> iterator = connections.iterator(); iterator.hasNext(); ) {
                PooledConnection connection = iterator.next();
                // Don't close a connection if it's still used
                if (connection.idle) {
                    iterator.remove();
                    IoUtils.safeClose(connection);
                }
            }
            // Is there any connection still used?
            if (connections.isEmpty()) {
                // No, invoked the stop callback
                onAllClosed.run();
            } else {
                // Yes, wait a bit and try close idle conections again
                XnioIoThread ioThread = (XnioIoThread) Thread.currentThread();
                ioThread.executeAfter(() -> closeAllConnections(onAllClosed), 500, MILLISECONDS);
            }
        }
    }

    /**
     * Wraps a {@link ClientConnection} with connection pool specific information.
     */
    private static class PooledConnection implements Closeable {
        private ClientConnection clientConnection;
        private boolean idle;
        private long createdOn = System.currentTimeMillis();

        private void sendRequest(ClientRequest request, ClientCallback<ClientExchange> clientCallback) {
            clientConnection.sendRequest(request, clientCallback);
        }

        private boolean isOpen() {
            return clientConnection.isOpen();
        }

        @Override
        public void close() throws IOException {
            clientConnection.close();
        }

        private boolean hasExpired(long now) {
            return createdOn + CONNECTION_TTL < now;
        }

        private boolean canReuse(long now) {
            return isOpen() && !hasExpired(now);
        }
    }

    /**
     * Wraps the callbacks which should be invoked when a connection is acquired of after too much time waiting.
     */
    private static class PooledConnectionWaiter {
        private final Consumer<PooledConnection> onGet;
        private final Runnable onTimeout;
        private final long timestamp;

        private PooledConnectionWaiter(Consumer<PooledConnection> onGet, Runnable onTimeout) {
            this.onGet = onGet;
            this.onTimeout = onTimeout;
            timestamp = System.currentTimeMillis();
        }
    }

    /**
     * Used by the {@link ConnectionPool} in order to setup connections.
     */
    private static class ConnectionFactory {
        private final URI kubernetesMasterUri;
        private final UndertowClient undertowClient;
        private final XnioSsl ssl;
        private final ByteBufferPool byteBufferPool;

        private ConnectionFactory(URI kubernetesMasterUri) {
            this.kubernetesMasterUri = kubernetesMasterUri;
            undertowClient = UndertowClient.getInstance();
            Xnio xnio = Xnio.getInstance(Undertow.class.getClassLoader());
            try {
                ssl = new UndertowXnioSsl(xnio, OptionMap.EMPTY);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            byteBufferPool = createByteBufferPool();
        }

        // The code here comes from the Wildfly source
        // It seems the team has spent quite some time searching for good defaults so let's reuse them.
        private ByteBufferPool createByteBufferPool() {
            long maxMemory = Runtime.getRuntime().maxMemory();
            boolean useDirectBuffers;
            int bufferSize, buffersPerRegion;
            if (maxMemory < 64 * 1024 * 1024) {
                //smaller than 64mb of ram we use 512b buffers
                useDirectBuffers = false;
                bufferSize = 512;
                buffersPerRegion = 10;
            } else if (maxMemory < 128 * 1024 * 1024) {
                //use 1k buffers
                useDirectBuffers = true;
                bufferSize = 1024;
                buffersPerRegion = 10;
            } else {
                //use 16k buffers for best performance
                //as 16k is generally the max amount of data that can be sent in a single write() call
                useDirectBuffers = true;
                bufferSize = 1024 * 16;
                buffersPerRegion = 20;
            }
            BufferAllocator<ByteBuffer> allocator;
            if (useDirectBuffers) {
                allocator = BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR;
            } else {
                allocator = BufferAllocator.BYTE_BUFFER_ALLOCATOR;
            }
            int maxRegionSize = buffersPerRegion * bufferSize;
            ByteBufferSlicePool pool = new ByteBufferSlicePool(allocator, bufferSize, maxRegionSize);
            return new XnioByteBufferPool(pool);
        }

        private void createConnection(ClientCallback<ClientConnection> callback) {
            XnioIoThread ioThread = (XnioIoThread) Thread.currentThread();
            undertowClient.connect(callback, kubernetesMasterUri, ioThread, ssl, byteBufferPool, OptionMap.EMPTY);
        }

        private void close() {
            byteBufferPool.close();
        }
    }
}
