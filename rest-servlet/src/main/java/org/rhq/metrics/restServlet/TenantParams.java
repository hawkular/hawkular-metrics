package org.rhq.metrics.restServlet;

import java.util.Map;

/**
 * @author John Sanda
 */
public class TenantParams {

    private String id;

    private Map<String, Integer> retentions;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, Integer> getRetentions() {
        return retentions;
    }

    public void setRetentions(Map<String, Integer> retentions) {
        this.retentions = retentions;
    }
}
