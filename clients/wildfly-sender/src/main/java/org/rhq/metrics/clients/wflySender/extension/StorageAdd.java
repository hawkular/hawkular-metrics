package org.rhq.metrics.clients.wflySender.extension;

import java.util.List;

import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

import org.rhq.metrics.clients.wflySender.service.RhqMetricsService;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

/**
 * Add a server node
 * @author Heiko W. Rupp
 */
class StorageAdd extends AbstractAddStepHandler {

    static final StorageAdd INSTANCE = new StorageAdd();

    private StorageAdd() {
    }

    @Override
    protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
        for (AttributeDefinition def : StorageDefinition.ATTRIBUTES) {
              def.validateAndSet(operation, model);
          }

    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model,
                                  ServiceVerificationHandler verificationHandler,
                                  List<ServiceController<?>> newControllers) throws OperationFailedException {

        final PathAddress address = PathAddress.pathAddress(operation.get(OP_ADDR));
        ModelNode fullTree = Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS));


        installService(context, address, fullTree, verificationHandler, newControllers);
    }

    static void installService(OperationContext context, PathAddress address,
                               ModelNode serverModel,
                               ServiceVerificationHandler verificationHandler,
                               List<ServiceController<?>> newControllers) throws OperationFailedException {

        String remoteServer  = address.getLastElement().getValue();

        // Add the service
        newControllers.add(
                RhqMetricsService.addService(context.getServiceTarget(), verificationHandler, remoteServer, serverModel));
    }

}
