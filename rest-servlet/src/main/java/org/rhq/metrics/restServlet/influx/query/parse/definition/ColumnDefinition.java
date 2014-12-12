package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public abstract class ColumnDefinition {
    private final boolean aliased;
    private final String alias;

    protected ColumnDefinition(String alias) {
        this.aliased = (alias != null);
        this.alias = alias;
    }

    public boolean isAliased() {
        return aliased;
    }

    public String getAlias() {
        return alias;
    }

    public abstract String getDisplayName();
}
