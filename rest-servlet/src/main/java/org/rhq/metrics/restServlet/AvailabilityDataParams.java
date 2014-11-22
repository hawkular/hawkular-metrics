package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class AvailabilityDataParams extends MetricDataParams {

    private Long timestamp;

    private String value;

    private List<AvailabilityDataPoint> data = new ArrayList<>();

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
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
            .add("metadata", metadata)
            .add("timestamp", timestamp)
            .add("value", value)
            .add("data", data)
            .toString();
    }

}
