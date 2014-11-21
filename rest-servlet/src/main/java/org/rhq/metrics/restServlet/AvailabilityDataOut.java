package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class AvailabilityDataOut {

    private String tenantId;

    private String name;

    private Map<String, String> attributes = new HashMap<>();

    private List<AvailabilityDataPoint> data = new ArrayList<>();

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

    public List<AvailabilityDataPoint> getData() {
        return data;
    }

    public void setData(List<AvailabilityDataPoint> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("tenantId", tenantId)
            .add("name", name)
            .add("attributes", attributes)
            .add("data", data)
            .toString();
    }

}
