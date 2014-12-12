package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class RawColumnDefinition extends ColumnDefinition {
    private final boolean prefixed;
    private final String prefix;
    private final String name;

    public RawColumnDefinition(String prefix, String name, String alias) {
        super(alias);
        this.prefixed = (prefix != null);
        this.prefix = prefix;
        this.name = name;
    }

    public boolean isPrefixed() {
        return prefixed;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return isAliased() ? getAlias() : name;
    }
}
