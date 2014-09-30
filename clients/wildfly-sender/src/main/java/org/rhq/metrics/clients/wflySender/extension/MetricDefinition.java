package org.rhq.metrics.clients.wflySender.extension;

import java.util.Arrays;
import java.util.Collection;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.PersistentResourceDefinition;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelType;

/**
 * Definition of a single metric
 * @author Heiko W. Rupp
 */
public class MetricDefinition extends PersistentResourceDefinition {

    public static final MetricDefinition INSTANCE = new MetricDefinition();

    public static final String METRIC  = "metric";

    static final SimpleAttributeDefinition PATH = new SimpleAttributeDefinitionBuilder("path", ModelType.STRING,false)
        .build();

    static final SimpleAttributeDefinition ATTRIBUTE  = new SimpleAttributeDefinitionBuilder("attribute", ModelType.STRING,false)
        .build();

    static AttributeDefinition[] ATTRIBUTES = {
        PATH,
        ATTRIBUTE
    };

    private MetricDefinition() {
        super(PathElement.pathElement(METRIC),
            SubsystemExtension.getResourceDescriptionResolver(METRIC),
            MetricAdd.INSTANCE,
            MetricRemove.INSTANCE
            );
    }

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
