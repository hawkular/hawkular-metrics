package org.rhq.metrics.clients.wflySender.service;

import org.jboss.as.controller.ModelController;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.as.server.ServerEnvironmentService;
import org.jboss.as.server.Services;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.jboss.threads.JBossThreadFactory;
import org.wildfly.metrics.scheduler.ModelControllerClientFactory;
import org.wildfly.metrics.scheduler.config.ConfigurationInstance;
import org.wildfly.metrics.scheduler.config.Interval;
import org.wildfly.metrics.scheduler.config.ResourceRef;
import org.wildfly.security.manager.action.GetAccessControlContextAction;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.security.AccessController.doPrivileged;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

/**
 * A service that gathers values from the system and sends them to a RHQ Metrics Server
 * @author Heiko W. Rupp
 */
public class RhqMetricsService implements Service<RhqMetricsService> {

    private boolean enabled;

    private ConfigurationInstance schedulerConfig;
    private org.wildfly.metrics.scheduler.Service schedulerService;

    private final InjectedValue<ModelController> modelControllerValue = new InjectedValue<>();
    private final InjectedValue<ServerEnvironment> serverEnvironmentValue = new InjectedValue<>();

    public static final ServiceName SERVICE_NAME = ServiceName.JBOSS.append("rhq-metrics", "sender");
    private boolean isStarted;

    public RhqMetricsService(ModelNode config, String remoteServer) {

        // the wildfly scheduler config
        this.schedulerConfig = new ConfigurationInstance(
                "localhost", // domain controller host
                9990 // domain controller port
        );

        int remotePort = config.get("port").asInt(8080);
        this.enabled = config.get("enabled").asBoolean(false);

        schedulerConfig.setRhqUrl("http://" + remoteServer + ":" + remotePort + "/rhq-metrics/metrics");

    }

    public static ServiceController<RhqMetricsService> addService(final ServiceTarget target,
                                                                  final ServiceVerificationHandler verificationHandler,
                                                                  String remoteServer, ModelNode config) {

        RhqMetricsService service = new RhqMetricsService(config, remoteServer);

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


        if (this.enabled) {

            final ThreadFactory threadFactory = new JBossThreadFactory(
                            new ThreadGroup("RHQ-Metrics-threads"),
                            Boolean.FALSE, null, "%G - %t", null, null,
                            doPrivileged(GetAccessControlContextAction.getInstance()));

            final ExecutorService executorService = Executors.newCachedThreadPool(threadFactory);
            final ModelControllerClient client = modelControllerValue.getValue().createClient(executorService);

            // create scheduler service
            schedulerService = new org.wildfly.metrics.scheduler.Service(schedulerConfig, new ModelControllerClientFactory() {
                @Override
                public ModelControllerClient createClient() {
                    return  modelControllerValue.getValue().createClient(executorService);
                }
            });

            // Get the server name from the runtime model
            boolean isDomainMode = getStringAttribute(client, "launch-type", PathAddress.EMPTY_ADDRESS).equalsIgnoreCase("domain");

            String hostName = null;
            String serverName = null;

            if(isDomainMode)
            {
                hostName = getStringAttribute(client, "local-host-name", PathAddress.EMPTY_ADDRESS);
                serverName = "tbd";   // TODO
            }
            else
            {
                hostName = "standalone";
                serverName = getStringAttribute(client, "name", PathAddress.EMPTY_ADDRESS);
            }



            // Create a http client
            schedulerService.start(hostName, serverName);
            schedulerService.reportEvery(30, TimeUnit.SECONDS);
        }

        this.isStarted = true;
    }

    @Override
    public void stop(StopContext stopContext) {
        if(schedulerService!=null)
            schedulerService.stop();

        this.isStarted = false;
    }

    @Override
    public RhqMetricsService getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }

    private String getStringAttribute(ModelControllerClient client, String attributeName, PathAddress path)  {

        try {
            ModelNode result = getModelNode(client,attributeName,path);
            return result.asString();
        } catch (IOException e) {
            return null;
        }
    }

    private ModelNode getModelNode(ModelControllerClient client, String attributeName, PathAddress address) throws IOException {
        ModelNode request = new ModelNode();

        if (address !=null) {

            request.get(ADDRESS).set(address.toModelNode());
        }

        request.get(OP).set(READ_ATTRIBUTE_OPERATION);
        request.get(NAME).set(attributeName);
        ModelNode resultNode = client.execute(request);
        // get the inner "result" -- should check for failure and so on...
        resultNode = resultNode.get("result");
        return resultNode;
    }

    public void addMetric(String metricName, ModelNode metricResource) {

        if(isStarted)
            throw new UnsupportedOperationException("Adding metric to running service is currently not supported");

        schedulerConfig.addResourceRef(
                new ResourceRef(
                        metricResource.get("path").asString(),
                        metricResource.get("attribute").asString(),
                        Interval.FIVE_SECONDS
                        )
        );
    }

    public void removeMetric(String metricName) {
       // TODO
    }
}
