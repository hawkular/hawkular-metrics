package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * @author John Sanda
 */
public class BucketedOutput {

    private String tenantId;

    private String name;

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    private Map<String, String> metadata = new HashMap<>();

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    private List<BucketDataPoint> data = new ArrayList<>();

    public BucketedOutput() {
    }

    public BucketedOutput(String tenantId, String name, Map<String, String> metadata) {
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

    public List<BucketDataPoint> getData() {
        return data;
    }

    public void setData(List<BucketDataPoint> data) {
        this.data = data;
    }

    public void add(BucketDataPoint d) {
        data.add(d);
    }

}
