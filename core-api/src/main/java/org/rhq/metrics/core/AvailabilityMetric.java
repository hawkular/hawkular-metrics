package org.rhq.metrics.core;

import java.util.Map;
import java.util.UUID;

/**
 * @author John Sanda
 */
public class AvailabilityMetric extends Metric<Availability> {

    public AvailabilityMetric(String tenantId, MetricId id) {
        super(tenantId, id);
    }

    public AvailabilityMetric(String tenantId, MetricId id, Map<String, String> metadata) {
        super(tenantId, id, metadata);
    }

    public AvailabilityMetric(String tenantId, MetricId id, Map<String, String> metadata, Integer dataRetention) {
        super(tenantId, id, metadata, dataRetention);
    }

    @Override
    public MetricType getType() {
        return MetricType.AVAILABILITY;
    }

    public void addAvailability(long timestamp, AvailabilityType availability) {
        addData(new Availability(this, timestamp, availability));
    }

    public void addAvailability(long timestamp, String availability) {
        addData(new Availability(this, timestamp, AvailabilityType.fromString(availability)));
    }

    public void addAvailability(UUID timeUUID, AvailabilityType availability) {
        addData(new Availability(this, timeUUID, availability));
    }
}
