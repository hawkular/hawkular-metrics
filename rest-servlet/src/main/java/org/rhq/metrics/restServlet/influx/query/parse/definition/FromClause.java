package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class FromClause {
    private final String name;
    private final boolean aliased;
    private final String alias;

    public FromClause(String name, String alias) {
        this.name = name;
        this.aliased = (alias != null);
        this.alias = alias;
    }

    public String getName() {
        return name;
    }

    public boolean isAliased() {
        return aliased;
    }

    public String getAlias() {
        return alias;
    }
}
