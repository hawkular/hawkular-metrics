package org.rhq.metrics.clients.wflySender.extension;

import org.jboss.as.controller.Extension;
import org.jboss.as.controller.ExtensionContext;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SubsystemRegistration;
import org.jboss.as.controller.descriptions.StandardResourceDescriptionResolver;
import org.jboss.as.controller.operations.common.GenericSubsystemDescribeHandler;
import org.jboss.as.controller.parsing.ExtensionParsingContext;
import org.jboss.as.controller.registry.ManagementResourceRegistration;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUBSYSTEM;


/**
 * @author <a href="kabir.khan@jboss.com">Kabir Khan</a>
 */
public class SubsystemExtension implements Extension {

    /**
     * The name space used for the {@code subsystem} element
     */
    public static final String NAMESPACE = "urn:org.rhq.metrics:wildflySender:1.0";

    /**
     * The name of our subsystem within the model.
     */
    public static final String SUBSYSTEM_NAME = "rhq-metrics-sender";

    /**
     * The parser used for parsing our subsystem.
     * See below
     */

    protected static final PathElement SUBSYSTEM_PATH = PathElement.pathElement(SUBSYSTEM, SUBSYSTEM_NAME);
    private static final String RESOURCE_NAME = SubsystemExtension.class.getPackage().getName() + ".LocalDescriptions";

    static StandardResourceDescriptionResolver getResourceDescriptionResolver(final String... keyPrefix) {
        StringBuilder prefix = new StringBuilder(SUBSYSTEM_NAME);
        if (keyPrefix!=null) {
            for (String kp : keyPrefix) {
                prefix.append('.').append(kp);
            }
        }
        return new StandardResourceDescriptionResolver(prefix.toString(), RESOURCE_NAME, SubsystemExtension.class.getClassLoader(), true, false);
    }

    @Override
    public void initializeParsers(ExtensionParsingContext context) {
        context.setSubsystemXmlMapping(SUBSYSTEM_NAME, NAMESPACE, SubsystemParser.INSTANCE);
    }


    @Override
    public void initialize(ExtensionContext context) {
        final SubsystemRegistration subsystem = context.registerSubsystem(SUBSYSTEM_NAME, 1, 0);
        final ManagementResourceRegistration registration = subsystem.registerSubsystemModel(SubsystemDefinition.INSTANCE);
        registration.registerOperationHandler(GenericSubsystemDescribeHandler.DEFINITION,
            GenericSubsystemDescribeHandler.INSTANCE);

        subsystem.registerXMLElementWriter(SubsystemParser.INSTANCE);
    }

}
