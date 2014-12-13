package org.rhq.metrics.core;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author John Sanda
 */
public class NumericMetric2 extends Metric<NumericData> {

    public NumericMetric2(String tenantId, MetricId id) {
        super(tenantId, id);
    }

    public NumericMetric2(String tenantId, MetricId id, Map<String, String> metadata) {
        super(tenantId, id, metadata);
    }

    public NumericMetric2(String tenantId, MetricId id, Map<String, String> metadata, Integer dataRetention) {
        super(tenantId, id, metadata, dataRetention);
    }

    @Override
    public MetricType getType() {
        return MetricType.NUMERIC;
    }

    public void addData(long timestamp, double value) {
        addData(new NumericData(this, timestamp, value));
    }

    public void addData(UUID timeUUID, double value) {
        addData(new NumericData(this, timeUUID, value));
    }

    public void addData(UUID timeUUID, double value, Set<Tag> tags) {
        addData(new NumericData(this, timeUUID, value, tags));
    }

}
