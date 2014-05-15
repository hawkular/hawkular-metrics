package org.rhq.metrics.restServlet;

/**
 * Just a simple representation of a Link
 * @author Heiko W. Rupp
 */
public class SimpleLink {
    private final String rel;
    private final String href;
    private final String title;

    public SimpleLink(String rel, String href, String title) {
        this.rel = rel;
        this.href = href;
        this.title = title;
    }

    public String getRel() {
        return rel;
    }

    public String getHref() {
        return href;
    }

    public String getTitle() {
        return title;
    }
}
