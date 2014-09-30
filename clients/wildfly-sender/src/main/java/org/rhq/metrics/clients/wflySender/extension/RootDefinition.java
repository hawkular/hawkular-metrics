package org.rhq.metrics.clients.wflySender.extension;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PersistentResourceDefinition;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;

/**
 * Definition of the &gt;subsystem element with one attribute
 * @author Heiko W. Rupp
 */
public class RootDefinition extends PersistentResourceDefinition {

    public static final RootDefinition INSTANCE = new RootDefinition();


    static PersistentResourceDefinition[] CHILDREN = {
        ServerDefinition.INSTANCE
    };


    private RootDefinition() {
        super(SubsystemExtension.SUBSYSTEM_PATH,
            SubsystemExtension.getResourceDescriptionResolver(),
            SubsystemAdd.INSTANCE,
            ReloadRequiredRemoveStepHandler.INSTANCE
            );
    }



    @Override
    public Collection<AttributeDefinition> getAttributes() {
        return Collections.emptySet();
    }

    @Override
    protected List<? extends PersistentResourceDefinition> getChildren() {
        return Arrays.asList(CHILDREN);
    }
}
