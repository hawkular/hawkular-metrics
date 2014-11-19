package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public abstract class ColumnDefinitionBuilder {
    protected String alias;

    public ColumnDefinitionBuilder setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public abstract ColumnDefinition createColumnDefinition();
}