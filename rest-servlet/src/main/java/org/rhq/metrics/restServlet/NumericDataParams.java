package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class NumericDataParams {

    private String tenantId;

    private String name;

    private Map<String, String> attributes = new HashMap<>();

    private long timestamp;

    private double value;

    private List<NumericDataPoint> data = new ArrayList<>();

    public NumericDataParams() {
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

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public List<NumericDataPoint> getData() {
        return data;
    }

    public void setData(List<NumericDataPoint> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("tenantId", tenantId)
            .add("name", name)
            .add("attributes", attributes)
            .add("timestamp", timestamp)
            .add("value", value)
            .add("data", data)
            .toString();
    }
}
