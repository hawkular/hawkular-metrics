package org.rhq.metrics.restServlet;

import java.util.Map;

/**
 * @author John Sanda
 */
public class TenantParams {

    private String id;

    private Map<String, String> retentionSettings;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, String> getRetentionSettings() {
        return retentionSettings;
    }

    public void setRetentionSettings(Map<String, String> retentionSettings) {
        this.retentionSettings = retentionSettings;
    }
}
