package org.rhq.metrics.clients.wflySender.extension;

import java.util.List;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PersistentResourceXMLDescription;
import org.jboss.as.controller.persistence.SubsystemMarshallingContext;
import org.jboss.dmr.ModelNode;
import org.jboss.staxmapper.XMLElementReader;
import org.jboss.staxmapper.XMLElementWriter;
import org.jboss.staxmapper.XMLExtendedStreamReader;
import org.jboss.staxmapper.XMLExtendedStreamWriter;

import static org.jboss.as.controller.PersistentResourceXMLDescription.builder;

/**
 * The subsystem parser, which uses stax to read and write to and from xml
 *
 * @author Heiko W. Rupp
 */
class SubsystemParser
        implements XMLStreamConstants, XMLElementReader<List<ModelNode>>, XMLElementWriter<SubsystemMarshallingContext> {

    public static final SubsystemParser INSTANCE = new SubsystemParser();

    private final static PersistentResourceXMLDescription xmlDescription;

    static {
        xmlDescription = builder(RootDefinition.INSTANCE)
                .addChild(
                        builder(StorageDefinition.INSTANCE)
                                .addAttributes(StorageDefinition.ATTRIBUTES)
                )
                .addChild(

                        builder(MonitorDefinition.INSTANCE)
                                .setXmlElementName(MonitorDefinition.MONITOR)
                                .addAttributes(MonitorDefinition.ATTRIBUTES)
                            .addChild(
                                builder(MetricDefinition.INSTANCE)
                                        .setXmlElementName(MetricDefinition.METRIC)
                                        .addAttributes(MetricDefinition.ATTRIBUTES)
                            )
                )
                .addChild(
                        builder(DiagnosticsDefinition.INSTANCE)
                                .addAttributes(DiagnosticsDefinition.ATTRIBUTES)
                )
                .build();

    }


    private SubsystemParser() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeContent(XMLExtendedStreamWriter writer, SubsystemMarshallingContext context) throws XMLStreamException {

        ModelNode model = new ModelNode();
        model.get(RootDefinition.INSTANCE.getPathElement().getKeyValuePair()).set(context.getModelNode());//this is bit of workaround for SPRD to work properly
        xmlDescription.persist(writer, model, SubsystemExtension.NAMESPACE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readElement(XMLExtendedStreamReader reader, List<ModelNode> list) throws XMLStreamException {
        xmlDescription.parse(reader, PathAddress.EMPTY_ADDRESS, list);
    }

}
