package org.rhq.metrics.clients.wflySender.extension;

import java.util.Arrays;
import java.util.Collection;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.PersistentResourceDefinition;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelType;

/**
 * &gtserver element inside the subsystem
 * @author Heiko W. Rupp
 */
public class ServerDefinition extends PersistentResourceDefinition {

    public static final ServerDefinition INSTANCE = new ServerDefinition();

    public static final String RHQM_SERVER = "rhqm-server";

    private ServerDefinition() {
        super(PathElement.pathElement(RHQM_SERVER),
            SubsystemExtension.getResourceDescriptionResolver(RHQM_SERVER),
            ServerAdd.INSTANCE,
            ServerRemove.INSTANCE
        );

    }

    static final SimpleAttributeDefinition ENABLED = new SimpleAttributeDefinitionBuilder("enabled", ModelType.BOOLEAN,false)
        .addFlag(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
        .build();


    static final SimpleAttributeDefinition PORT = new SimpleAttributeDefinitionBuilder("port", ModelType.INT,false)
        .addFlag(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
        .setAllowExpression(true)
        .build();

    static final SimpleAttributeDefinition TOKEN = new SimpleAttributeDefinitionBuilder("token", ModelType.STRING,false)
        .addFlag(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
        .build();


    static final AttributeDefinition[] ATTRIBUTES = {
        ENABLED,
        PORT,
        TOKEN
    };

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {

        ServerWriteAttributeHandler handler = new ServerWriteAttributeHandler(ATTRIBUTES);

        for (AttributeDefinition attr : ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, handler);
        }
    }

    @Override
    public Collection<AttributeDefinition> getAttributes() {
        return Arrays.asList(ATTRIBUTES);
    }


}
