package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    private List<DataPointOut> data = new ArrayList<>();

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    private Integer dataRetention;

    public MetricOut() {
    }

    public MetricOut(String tenantId, String name, Map<String, String> metadata) {
        this.tenantId = tenantId;
        this.name = name;
        this.metadata = metadata;
    }

    public MetricOut(String tenantId, String name, Map<String, String> metadata, Integer dataRetention) {
        this.tenantId = tenantId;
        this.name = name;
        this.metadata = metadata;
        this.dataRetention = dataRetention;
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

    public List<DataPointOut> getData() {
        return data;
    }

    public void setData(List<DataPointOut> data) {
        this.data = data;
    }

    public Integer getDataRetention() {
        return dataRetention;
    }

    public void setDataRetention(Integer dataRetention) {
        this.dataRetention = dataRetention;
    }
}
