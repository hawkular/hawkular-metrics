package org.rhq.metrics.clients.wflySender.extension;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.PersistentResourceDefinition;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelType;

/**
 * Definition of a monitor
 * @author Heiko Braun
 */
public class MonitorDefinition extends PersistentResourceDefinition {

    public static final MonitorDefinition INSTANCE = new MonitorDefinition();

    public static final String MONITOR = "server-monitor";

    static final SimpleAttributeDefinition ENABLED = new SimpleAttributeDefinitionBuilder("enabled", ModelType.BOOLEAN,false)
        .build();

    static final SimpleAttributeDefinition THREADS  = new SimpleAttributeDefinitionBuilder("num-threads", ModelType.INT,false)
            .build();


    static AttributeDefinition[] ATTRIBUTES = {
            ENABLED, THREADS
    };

    private MonitorDefinition() {
        super(PathElement.pathElement(MONITOR),
            SubsystemExtension.getResourceDescriptionResolver(MONITOR),
            MonitorAdd.INSTANCE,
            MonitorRemove.INSTANCE
            );
    }

    @Override
    protected List<? extends PersistentResourceDefinition> getChildren() {
        return Arrays.asList(MetricDefinition.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        MonitorWriteAttributeHandler handler = new MonitorWriteAttributeHandler(ATTRIBUTES);

        for (AttributeDefinition attr : ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, handler);
        }

    }

    @Override
    public Collection<AttributeDefinition> getAttributes() {
        return Arrays.asList(ATTRIBUTES);
    }
}
