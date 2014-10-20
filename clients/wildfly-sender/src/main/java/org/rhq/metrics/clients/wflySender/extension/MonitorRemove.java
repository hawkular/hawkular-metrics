package org.rhq.metrics.clients.wflySender.extension;

import org.jboss.as.controller.AbstractRemoveStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;


import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

/**
 * Removes a monitor from the system
 * @author Heiko Braun
 */
public class MonitorRemove extends AbstractRemoveStepHandler {

    public static final MonitorRemove INSTANCE = new MonitorRemove();

    private MonitorRemove() {

    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation,
                                  ModelNode model) throws OperationFailedException {
        final PathAddress address = PathAddress.pathAddress(operation.get(OP_ADDR));

        /*String metricName = address.getLastElement().getValue();
        RhqMonitorsService service = (RhqMonitorsService) context.getServiceRegistry(true).getService(RhqMonitorsService.SERVICE_NAME).getService();
        service.removeMonitor(metricName);*/

    }
}
