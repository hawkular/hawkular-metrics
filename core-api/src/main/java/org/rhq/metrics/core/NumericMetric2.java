package org.rhq.metrics.core;

import java.util.Map;
import java.util.UUID;

/**
 * @author John Sanda
 */
public class NumericMetric2 extends Metric<NumericData> {

    public NumericMetric2(String tenantId, MetricId id) {
        super(tenantId, id);
    }

    public NumericMetric2(String tenantId, MetricId id, Map<String, String> attributes) {
        super(tenantId, id, attributes);
    }

    public void addData(long timestamp, double value) {
        addData(new NumericData(this, timestamp, value));
    }

    public void addData(UUID timeUUID, double value) {
        addData(new NumericData(this, timeUUID, value));
    }

}
