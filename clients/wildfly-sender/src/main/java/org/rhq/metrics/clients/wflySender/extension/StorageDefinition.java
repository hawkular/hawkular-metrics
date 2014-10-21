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
 * storage adapter configuration.
 *
 * @author Heiko W. Rupp
 */
public class StorageDefinition extends PersistentResourceDefinition {

    public static final StorageDefinition INSTANCE = new StorageDefinition();

    public static final String STORAGE_ADAPTER = "storage-adapter";

    private StorageDefinition() {
        super(PathElement.pathElement(STORAGE_ADAPTER),
                SubsystemExtension.getResourceDescriptionResolver(STORAGE_ADAPTER),
                StorageAdd.INSTANCE,
                StorageRemove.INSTANCE
        );

    }

    static final SimpleAttributeDefinition URL = new SimpleAttributeDefinitionBuilder("url", ModelType.STRING,false)
            .addFlag(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
            .setAllowExpression(true)
            .build();

    static final SimpleAttributeDefinition USER = new SimpleAttributeDefinitionBuilder("user", ModelType.STRING,true)
            .addFlag(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
            .setAllowExpression(true)
            .build();

    static final SimpleAttributeDefinition PASSWORD = new SimpleAttributeDefinitionBuilder("password", ModelType.STRING,true)
            .addFlag(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
            .setAllowExpression(true)
            .build();

    static final SimpleAttributeDefinition TOKEN = new SimpleAttributeDefinitionBuilder("token", ModelType.STRING,true)
            .addFlag(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
            .build();

    static final SimpleAttributeDefinition DB = new SimpleAttributeDefinitionBuilder("db", ModelType.STRING,true)
                .addFlag(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .build();


    static final AttributeDefinition[] ATTRIBUTES = {
            URL,
            USER, PASSWORD,
            TOKEN, DB
    };

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {

        StorageWriteAttributeHandler handler = new StorageWriteAttributeHandler(ATTRIBUTES);

        for (AttributeDefinition attr : ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, handler);
        }
    }

    @Override
    public Collection<AttributeDefinition> getAttributes() {
        return Arrays.asList(ATTRIBUTES);
    }


}
