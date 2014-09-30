package org.rhq.metrics.clients.wflySender.extension;

import org.jboss.as.controller.AbstractRemoveStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.dmr.ModelNode;

import org.rhq.metrics.clients.wflySender.service.RhqMetricsService;

/**
 * Handler responsible for removing the subsystem resource from the model
 *
 * @author <a href="kabir.khan@jboss.com">Kabir Khan</a>
 */
class ServerRemove extends AbstractRemoveStepHandler {

    static final ServerRemove INSTANCE = new ServerRemove();


    private ServerRemove() {
    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
        //Remove any services installed by the corresponding add handler here
        context.removeService(RhqMetricsService.SERVICE_NAME);
    }


}
