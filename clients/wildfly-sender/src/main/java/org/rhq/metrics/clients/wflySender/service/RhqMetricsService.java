package org.rhq.metrics.clients.wflySender.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.wildfly.security.manager.action.GetAccessControlContextAction;

import org.jboss.as.controller.ModelController;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.as.server.ServerEnvironmentService;
import org.jboss.as.server.Services;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.Property;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.jboss.threads.JBossThreadFactory;

import org.rhq.metrics.client.common.Batcher;
import org.rhq.metrics.client.common.MetricType;
import org.rhq.metrics.client.common.SingleMetric;

import static java.security.AccessController.doPrivileged;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADDRESS;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.READ_ATTRIBUTE_OPERATION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.READ_RESOURCE_DESCRIPTION_OPERATION;

/**
 * A service that gathers values from the system and sends them to a RHQ Metrics Server
 * @author Heiko W. Rupp
 */
public class RhqMetricsService implements Service<RhqMetricsService> {

    private static final String WILDFLY_PATH_ELEMENT = ".wildfly.";
    private boolean enabled;
    private String token;
    private String rhqmServerUri;
    private String hostPrefix;

    private final InjectedValue<ModelController> modelControllerValue = new InjectedValue<>();
    public ModelControllerClient controllerClient;
    HttpClient httpclient ;
    private final InjectedValue<ServerEnvironment> serverEnvironmentValue = new InjectedValue<>();

    public static final ServiceName SERVICE_NAME = ServiceName.JBOSS.append("rhq-metrics", "sender");
    private Map<String, MetricDef> metricDefMap = new HashMap<>();

    public RhqMetricsService(
        ModelNode config, String serverName) {
        int port = config.get("port").asInt(8080);
        this.enabled = config.get("enabled").asBoolean(false);
        this.token = config.get("token").asString();

        rhqmServerUri = "http://" + serverName + ":" + port + "/rhq-metrics/metrics";

    }

    public static ServiceController<RhqMetricsService> addService(final ServiceTarget target,
                                                                  final ServiceVerificationHandler verificationHandler,
                                                                  String serverName, ModelNode config) {

        RhqMetricsService service = new RhqMetricsService(config, serverName);
        return target.addService(SERVICE_NAME, service)
            .addDependency(ServerEnvironmentService.SERVICE_NAME, ServerEnvironment.class,
                service.serverEnvironmentValue)
                .addDependency(Services.JBOSS_SERVER_CONTROLLER, ModelController.class,
                    service.modelControllerValue)
                .addListener(verificationHandler)
                .setInitialMode(ServiceController.Mode.ACTIVE)
                .install();
        }


    @Override
    public void start(StartContext startContext) throws StartException {

        final ThreadFactory threadFactory = new JBossThreadFactory(
                        new ThreadGroup("RHQ-Metrics-threads"),
                        Boolean.FALSE, null, "%G - %t", null, null,
                        doPrivileged(GetAccessControlContextAction.getInstance()));
        ExecutorService executorService = Executors.newCachedThreadPool(threadFactory);
        controllerClient = modelControllerValue.getValue().createClient(executorService);

        if (this.enabled) {
            // Get the server name from the runtime model
            hostPrefix = getStringAttribute("name",PathAddress.EMPTY_ADDRESS);

            // Create a http client
            httpclient = new DefaultHttpClient();

            // And start the worker thread
            WORKER.start();
        }
    }

    @Override
    public void stop(StopContext stopContext) {
        if (WORKER!=null) {
            WORKER.interrupt();
        }
    }

    @Override
    public RhqMetricsService getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }



    private Thread WORKER = new Thread("RHQ-Metrics-Sender-Thread") {
        @Override
        public void run() {
            while(true) {
                try {

                    long now = System.currentTimeMillis();
                    List<SingleMetric> metrics = new ArrayList<>();

                    // Read the metrics from the model
                    for (MetricDef def : metricDefMap.values()) {
                        obtainMetricValue(def,metrics, now);
                    }

                    // If we have data, send it to the RHQ Metrics server
                    if (metrics.size()>0) {
                        HttpPost post = new HttpPost(rhqmServerUri);
                        post.setHeader("Content-Type", "application/json;charset=utf-8");
                        post.setEntity(new StringEntity(Batcher.metricListToJson(metrics)));
                        HttpResponse httpResponse = httpclient.execute(post);
                        StatusLine statusLine = httpResponse.getStatusLine();
                        if (statusLine.getStatusCode() != 200) {
                            System.err.println("  +-- Result status is " + statusLine);
                        }
                        post.releaseConnection();
                    }


                } catch (IllegalArgumentException | IOException iae) {
                    iae.printStackTrace();
                }

                try {
                    Thread.sleep(60*1000L);
                } catch (InterruptedException e) {
                    interrupted();
                    break;
                }
            }
        }
    };

    private void obtainMetricValue(MetricDef def, List<SingleMetric> metrics, long now) throws IOException {
        switch (def.type) {
        case OBJECT:
            attributeMapToMetrics(now, metrics, def.attribute, def.path);
            break;
        case INT:
            attributeNumToMetrics(now, metrics, def.attribute, def.path);
            break;
        case LONG:
            attributeNumToMetrics(now, metrics, def.attribute, def.path);
            break;
        case FAILED:
            // Re check the type info. Server may not have been fully up during initial check
            // Only check every 3 minutes
            if (def.failCount < 5 && (System.currentTimeMillis() - def.lastTypeCheck) > 3L* 60L * 1000) {
                getTypeFromModel(def);
            }
            if (def.failCount>5) {
                System.err.println("Can not resolve type for " + def + " no longer retrying...");
                def.type=MetricSubType.UNKNOWN;
            }
            break;
        case UNKNOWN:
            // Nothing to do for us
            return;
        default:
            throw new IOException("Metric type for " + def + " not known");
        }
    }

    private void attributeNumToMetrics(long now, List<SingleMetric> metrics, String attributeName, PathAddress path) throws  IOException {
        Double d = getDoubleAttribute(attributeName, path);

        String source = hostPrefix + WILDFLY_PATH_ELEMENT + attributeName;
        SingleMetric m = new SingleMetric(source,now,d);
        metrics.add(m);
    }

    private void attributeMapToMetrics(long now, List<SingleMetric> metrics, String attributeName, PathAddress path) throws IOException {
        List<Property> props = getMapAttribute(attributeName, path);

        for (Property property : props) {
            String source = hostPrefix + WILDFLY_PATH_ELEMENT + attributeName + "." + property.getName();
            ModelNode resultNode = property.getValue();
            SingleMetric metric = new SingleMetric(source,now,resultNode.asDouble());
            metric.setMetricType(MetricType.SIMPLE);
            metrics.add(metric);
        }
    }

    private String getStringAttribute(String attributeName, PathAddress path)  {

        try {
            ModelNode result = getModelNode(attributeName,path);
            return result.asString();
        } catch (IOException e) {
            return null;
        }
    }

    private Double getDoubleAttribute(String attributeName, PathAddress path)  {

        try {
            ModelNode result = getModelNode(attributeName,path);
            return result.asDouble();
        } catch (IOException e) {
            return null;
        }
    }

    private List<Property> getMapAttribute(String attributeName, PathAddress path) throws IOException {
        ModelNode result = getModelNode(attributeName,path);
        return result.asPropertyList();
    }

    private ModelNode getModelNode(String attributeName, PathAddress address) throws IOException {
        ModelNode request = new ModelNode();

        if (address !=null) {

            request.get(ADDRESS).set(address.toModelNode());
        }

        request.get(OP).set(READ_ATTRIBUTE_OPERATION);
        request.get(NAME).set(attributeName);
        ModelNode resultNode = controllerClient.execute(request);
        // get the inner "result" -- should check for failure and so on...
        resultNode = resultNode.get("result");
        return resultNode;
    }

    public void addMetric(String metricName, ModelNode fullTree) {
        MetricDef def = new MetricDef();
        def.name=metricName;
        def.attribute = fullTree.get("attribute").asString();
        String tmp = fullTree.get("path").asString();

        def.path = pathStringToAddress(tmp);

        getTypeFromModel(def);

        metricDefMap.put(metricName, def);
    }

    public void removeMetric(String metricName) {
        metricDefMap.remove(metricName);
    }


    /**
     * Read the type of the defined metric (attribute) from the model and
     * put it into the passed definition
     * @param def Partially filled metric definition object
     */
    private void getTypeFromModel(MetricDef def) {
        ModelNode request = new ModelNode();

        def.lastTypeCheck = System.currentTimeMillis();
        if (def.path ==null) {
            def.type = MetricSubType.UNKNOWN;
            return;
        }


        // This can be null in the test suite and may be null in some rare cases in the server
        if (controllerClient==null) {
            def.type = MetricSubType.FAILED;
            def.failCount++;
            return;
        }

        ModelNode addressNode = new ModelNode();
        addressNode.add(def.path.toModelNode());
        request.get(ADDRESS).set(def.path.toModelNode());

        request.get(OP).set(READ_RESOURCE_DESCRIPTION_OPERATION);

        try {
            ModelNode resultNode;
            resultNode = controllerClient.execute(request);

            String outcome = resultNode.get("outcome").asString();
            if (!"success".equals(outcome)) {
                def.type = MetricSubType.FAILED;
                def.failCount++;
                return;
            }

            // get the inner "result" -- should check for failure and so on...
            resultNode = resultNode.get("result");

            ModelNode attNode = resultNode.get("attributes");
            attNode = attNode.get(def.attribute);

            String type = attNode.get("type").asString();

            try {
                def.type = MetricSubType.valueOf(type);
            } catch (IllegalArgumentException e) {
                System.err.println("  Error " + type + " not known for " + def);
                def.type=MetricSubType.UNKNOWN;
            }

        } catch (IOException e) {
            def.type = MetricSubType.UNKNOWN;
        }
    }

    /**
     * Turn a path in the String form /bla=fasel/foo=bar into a PathAddress
     * @param in A path string
     * @return a PathAddress that may be empty.
     */
    private PathAddress pathStringToAddress(String in) {
        if (in==null || in.isEmpty()) {
            return PathAddress.EMPTY_ADDRESS;
        }

        if (in.startsWith("/")) {
            in= in.substring(1);
        }
        if (in.endsWith("/")) {
            in = in.substring(0,in.length()-1);
        }

        String[] pairs = in.split(Pattern.quote("/"));
        List<PathElement> elements = new ArrayList<>(pairs.length);

        for (String pair : pairs) {
            String[] parts = pair.split("=");

            PathElement element = PathElement.pathElement(parts[0],parts[1]);
            elements.add(element);
        }
        return PathAddress.pathAddress(elements);

    }


    private static class MetricDef {
        PathAddress path;
        String attribute;
        String name;
        MetricSubType type;
        long lastTypeCheck;
        int failCount =0;

        @Override
        public String toString() {
            return "MetricDefinition{" +
                "name='" + name + '\'' +
                ", path=" + path + '\'' +
                ", attribute='" + attribute + '\'' +
                ", type='" + type + '\'' +
                '}';
        }
    }

    private enum MetricSubType {
        INT, LONG,
        STRING,
        OBJECT, // a map
        FAILED, // retrieval failed
        UNKNOWN
    }
}
