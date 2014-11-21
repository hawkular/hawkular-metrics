package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class NumericDataParams extends MetricDataParams {

    private Long timestamp;

    private Double value;

    private List<NumericDataPoint> data = new ArrayList<>();

    public NumericDataParams() {
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
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
