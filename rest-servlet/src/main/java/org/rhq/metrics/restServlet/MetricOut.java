package org.rhq.metrics.restServlet;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;


/**
 * @author John Sanda
 */
public class MetricOut {

    private String tenantId;

    private String name;

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    private Map<String, String> metadata = new HashMap<>();

    public MetricOut() {
    }

    public MetricOut(String tenantId, String name, Map<String, String> metadata) {
        this.tenantId = tenantId;
        this.name = name;
        this.metadata = metadata;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }
}
