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

    static final SimpleAttributeDefinition RESOURCE = new SimpleAttributeDefinitionBuilder("resource", ModelType.STRING,false)
        .build();

    static final SimpleAttributeDefinition ATTRIBUTE  = new SimpleAttributeDefinitionBuilder("attribute", ModelType.STRING,false)
        .build();

    static final SimpleAttributeDefinition SECONDS  = new SimpleAttributeDefinitionBuilder("seconds", ModelType.INT,true)
           .build();

    static final SimpleAttributeDefinition MINUTES  = new SimpleAttributeDefinitionBuilder("minutes", ModelType.INT,true)
               .build();

    static final SimpleAttributeDefinition HOURS  = new SimpleAttributeDefinitionBuilder("hours", ModelType.INT,true)
               .build();

    static AttributeDefinition[] ATTRIBUTES = {
            RESOURCE,
            ATTRIBUTE,
            SECONDS,
            MINUTES,
            HOURS
    };

    private MetricDefinition() {
        super(PathElement.pathElement(METRIC),
            SubsystemExtension.getResourceDescriptionResolver(MonitorDefinition.MONITOR, METRIC),
            MetricAdd.INSTANCE,
            MetricRemove.INSTANCE
            );
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        MetricWriteAttributeHandler handler = new MetricWriteAttributeHandler(ATTRIBUTES);

        for (AttributeDefinition attr : ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, handler);
        }

    }

    @Override
    public Collection<AttributeDefinition> getAttributes() {
        return Arrays.asList(ATTRIBUTES);
    }
}
