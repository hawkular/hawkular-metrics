package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class RawColumnDefinitionBuilder extends ColumnDefinitionBuilder {
    private String prefix;
    private String name;

    public RawColumnDefinitionBuilder setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public RawColumnDefinitionBuilder setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public ColumnDefinition createColumnDefinition() {
        return new RawColumnDefinition(prefix, name, alias);
    }
}