package org.rhq.metrics.clients.wflySender.extension;

import java.util.ArrayList;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.RestartParentWriteAttributeHandler;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;

import org.rhq.metrics.clients.wflySender.service.RhqMetricsService;

/**
 * Handler that restarts the service on attribute changes
 * @author Heiko W. Rupp
 */
public class ServerWriteAttributeHandler extends RestartParentWriteAttributeHandler {

    public ServerWriteAttributeHandler(AttributeDefinition... definitions) {
        super(ServerDefinition.RHQM_SERVER, definitions);
    }

    @Override
    protected void recreateParentService(OperationContext context, PathAddress parentAddress, ModelNode parentModel,
                                         ServiceVerificationHandler verificationHandler) throws OperationFailedException {
        ServerAdd.installService(context,parentAddress,parentModel,verificationHandler, new ArrayList<ServiceController<?>>());
    }

    @Override
    protected ServiceName getParentServiceName(PathAddress parentAddress) {
        return RhqMetricsService.SERVICE_NAME.append(parentAddress.getLastElement().getValue());
    }
}
