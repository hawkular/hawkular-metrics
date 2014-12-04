package org.rhq.metrics.restServlet;

import java.util.HashMap;
import java.util.Map;

/**
 * @author John Sanda
 */
// TODO rename class to better reflect it is used for input and output
public class TenantParams {

    private String id;

    private Map<String, Integer> retentions = new HashMap<>();

    public TenantParams() {
    }

    public TenantParams(String id, Map<String, Integer> retentions) {
        this.id = id;
        this.retentions = retentions;
    }

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
