package org.rhq.metrics.restServlet;

/**
 * Just a simple representation of a Link
 * @author Heiko W. Rupp
 */
public class SimpleLink {
    private String rel;
    private String href;

    public SimpleLink(String rel, String href) {
        this.rel = rel;
        this.href = href;
    }

    public String getRel() {
        return rel;
    }

    public void setRel(String rel) {
        this.rel = rel;
    }

    public String getHref() {
        return href;
    }

    public void setHref(String href) {
        this.href = href;
    }
}
