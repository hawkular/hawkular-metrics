package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class NameFunctionArgument implements FunctionArgument {
    private final boolean prefixed;
    private final String prefix;
    private final String name;

    public NameFunctionArgument(String prefix, String name) {
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
}
