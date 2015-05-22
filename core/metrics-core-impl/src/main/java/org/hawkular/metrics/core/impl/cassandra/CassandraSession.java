/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.impl.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.hawkular.metrics.schema.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * @author John Sanda
 * @author Matt Wringe
 */
public class CassandraSession {

    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceCassandra.class);

    private static final String CASSANDRA_STORAGE_SERVICE = "org.apache.cassandra.db:type=StorageService";

    private Optional<Session> session;

    private final ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    ListenableFuture<Session> listenableFuture;
    Callable<Session> asyncTask;

    public static class Builder {

        private final Map<String, String> options;
        private FutureCallback<Session> callback = null;

        public Builder() {
            String cassandraCqlPortString = System.getenv("CASSANDRA_CQL_PORT");
            if (cassandraCqlPortString == null) {
                cassandraCqlPortString = System.getProperty("hawkular-metrics.cassandra-cql-port", "9042");
            }

            String cassandraNodes = System.getenv("CASSANDRA_NODES");
            if (cassandraNodes == null) {
                cassandraNodes = System.getProperty("hawkular-metrics.cassandra-nodes", "127.0.0.1");
            }

            options = new HashMap<>();
            options.put("cqlport", cassandraCqlPortString);
            options.put("nodes", cassandraNodes);
            options.put("keyspace", System.getProperty("cassandra.keyspace", "hawkular_metrics"));
        }

        public Builder withOptions(Map<String, String> options) {
            this.options.putAll(options);
            return this;
        }

        public Builder withCQLPort(int port) {
            options.put("cqlport", Integer.toString(port));
            return this;
        }

        public Builder withKeyspace(String keyspace) {
            options.put("keyspace", keyspace);
            return this;
        }

        public Builder withNodes(String... nodes) {
            StringBuilder buffer = new StringBuilder();
            for (String node : nodes) {
                buffer.append(node).append(",");
            }
            if (buffer.length() > 0) {
                buffer.deleteCharAt(buffer.length() - 1);
            }
            options.put("nodes", buffer.toString());
            return this;
        }

        public Builder withInitializationCallback(FutureCallback<Session> callback) {
            this.callback = callback;
            return this;
        }

        public CassandraSession build() {
            CassandraSession cassandraSession = new CassandraSession(options, callback);
            return cassandraSession;
        }
    }

    private CassandraSession(Map<String, String> params, FutureCallback<Session> callback) {
        asyncTask = new Callable<Session>() {
            @Override
            public Session call() throws Exception {
                String tmp = params.get("cqlport");
                int port = 9042;
                try {
                    port = Integer.parseInt(tmp);
                } catch (NumberFormatException nfe) {
                    logger.warn("Invalid context param 'cqlport', not a number. Will use a default of 9042");
                }

                String[] nodes;
                if (params.containsKey("nodes")) {
                    nodes = params.get("nodes").split(",");
                } else {
                    nodes = new String[] {"127.0.0.1"};
                }

                if (isEmbeddedCassandraServer()) {
                    verifyNodeIsUp(nodes[0], 9990, 10, 1000);
                }

                String keyspace = params.get("keyspace");
                if (keyspace==null||keyspace.isEmpty()) {
                    logger.debug("No keyspace given in params, checking system properties ...");
                    keyspace = System.getProperty("cassandra.keyspace");
                }

                if (keyspace==null||keyspace.isEmpty()) {
                    logger.debug("No explicit keyspace given, will default to 'hawkular'");
                    keyspace = "hawkular_metrics";
                }

                logger.info("Using a key space of '" + keyspace + "'");


                //TODO: move this to be asyncronous?
                session = getCassandraSession(nodes, port, 1000);
                if (session == null) {
                    throw new RuntimeException("Could not access the Cassandra cluster. Terminating.");
                }

                if (System.getProperty("cassandra.resetdb")!=null) {
                    // We want a fresh DB -- mostly used for tests
                    dropKeyspace(keyspace);
                }
                // This creates/updates the keyspace + tables if needed
                updateSchemaIfNecessary(keyspace);
                session.get().execute("USE " + keyspace);

                return session.get();
            }
        };

        listenableFuture = executorService.submit(asyncTask);

        if (callback != null) {
            Futures.addCallback(listenableFuture, callback);
        }
    }

    private boolean isEmbeddedCassandraServer() {
        try {
            MBeanServerConnection serverConnection = ManagementFactory.getPlatformMBeanServer();
            ObjectName storageService = new ObjectName(CASSANDRA_STORAGE_SERVICE);
            MBeanInfo storageServiceInfo = serverConnection.getMBeanInfo(storageService);

            if (storageServiceInfo != null) {
                return true;
            }

            return false;
        } catch (Exception e) {
            return false;
        }
    }

    boolean verifyNodeIsUp(String address, int jmxPort, int retries, long timeout) {
        Boolean nativeTransportRunning = false;
        Boolean initialized = false;
        for (int i = 0; i < retries; ++i) {
            if (i > 0) {
                try {
                    // cycle between original and more wait time - avoid waiting huge amounts of time
                    long sleepMillis = timeout * (1 + ((i - 1) % 4));
                    logger.info("[" + i + "/" + (retries - 1) + "] Retrying storage node status check in ["
                            + sleepMillis + "]ms...");
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e1) {
                    logger.error("Failed to get storage node status.", e1);
                    return false;
                }
            }
            try {
                MBeanServerConnection serverConnection = ManagementFactory.getPlatformMBeanServer();
                ObjectName storageService = new ObjectName(CASSANDRA_STORAGE_SERVICE);
                nativeTransportRunning = (Boolean) serverConnection.getAttribute(storageService,
                        "NativeTransportRunning");
                initialized = (Boolean) serverConnection.getAttribute(storageService,
                        "Initialized");
                if (nativeTransportRunning && initialized) {
                    logger.info("Successfully verified that the storage node is initialized and running!");
                    return true; // everything is up, get out of our wait loop and immediately return
                }
                logger.info("Storage node is still initializing. NativeTransportRunning=[" + nativeTransportRunning
                        + "], Initialized=[" + initialized + "]");
            } catch (Exception e) {
                logger.warn("Cannot get storage node status - assuming it is not up yet. Cause: "
                        + ((e.getCause() == null) ? e : e.getCause()));
            }
        }
        logger.error("Cannot verify that the storage node is up.");
        return false;
    }

    Optional<Session> getCassandraSession(String[] nodes, int port, long timeout) {
        int attempts = 0;
        Optional<Session> session = null;

        while(session == null && !Thread.currentThread().isInterrupted()) {
            attempts++;
            try {
                Cluster cluster = new Cluster.Builder()
                        .addContactPoints(nodes)
                        .withPort(port)
                        .build();
                session = Optional.of(cluster.connect("system"));
                return session;
            } catch (Exception e) {
                logger.warn("Could not connect to Cassandra cluster - assuming its not up yet. Cause :"
                        + ((e.getCause() == null) ? e : e.getCause()));
            }

            if (session == null) {
                try {
                    // cycle between original and more wait time - avoid waiting huge amounts of time
                    long sleepMillis = timeout * (1 + ((attempts - 1) % 4));
                    logger.warn("[" + attempts + "] Retrying connecting to Cassandra cluster in ["
                            + sleepMillis + "]ms...");
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    logger.error("Failure trying to connect to the Cassandra cluster.", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        return session;
    }

    private void dropKeyspace(String keyspace) {
        session.get().execute("DROP KEYSPACE IF EXISTS " + keyspace);
    }

    private void updateSchemaIfNecessary(String schemaName) {
        try {
            SchemaManager schemaManager = new SchemaManager(session.get());
            schemaManager.createSchema(schemaName);
        } catch (IOException e) {
            throw new RuntimeException("Schema creation failed", e);
        }
    }

    public void shutdown() {
        if(session.isPresent()) {
            Session s = session.get();
            s.close();
            s.getCluster().close();
        }
    }

}
