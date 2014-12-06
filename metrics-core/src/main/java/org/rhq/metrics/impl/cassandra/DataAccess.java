package org.rhq.metrics.impl.cassandra;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSetFuture;

import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricData;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric2;
import org.rhq.metrics.core.Tenant;

/**
 * @author John Sanda
 */
public interface DataAccess {
        ResultSetFuture insertTenant(Tenant tenant);

    ResultSetFuture findAllTenantIds();

    ResultSetFuture findTenant(String id);

    ResultSetFuture insertMetric(Metric metric);

    ResultSetFuture findMetric(String tenantId, MetricType type, MetricId id, long dpart);

    ResultSetFuture addMetadata(Metric metric);

    ResultSetFuture updateMetadata(Metric metric, Map<String, String> additions, Set<String> removals);

    ResultSetFuture updateMetadataInMetricsIndex(Metric metric, Map<String, String> additions,
        Set<String> deletions);

    <T extends Metric> ResultSetFuture updateMetricsIndex(List<T> metrics);

    ResultSetFuture findMetricsInMetricsIndex(String tenantId, MetricType type);

    ResultSetFuture insertData(NumericMetric2 metric, int ttl);

    ResultSetFuture findData(NumericMetric2 metric, long startTime, long endTime);

    ResultSetFuture findData(NumericMetric2 metric, long startTime, long endTime, boolean includeWriteTime);

    ResultSetFuture findData(NumericMetric2 metric, long timestamp, boolean includeWriteTime);

    ResultSetFuture findData(AvailabilityMetric metric, long startTime, long endTime);

    ResultSetFuture findData(AvailabilityMetric metric, long startTime, long endTime, boolean includeWriteTime);

    ResultSetFuture findData(AvailabilityMetric metric, long timestamp);

    ResultSetFuture deleteNumericMetric(String tenantId, String metric, Interval interval, long dpart);

    ResultSetFuture findAllNumericMetrics();

    ResultSetFuture insertNumericTag(String tag, List<NumericData> data);

    ResultSetFuture insertAvailabilityTag(String tag, List<Availability> data);

    ResultSetFuture updateDataWithTag(MetricData data, Set<String> tags);

    ResultSetFuture findNumericDataByTag(String tenantId, String tag);

    ResultSetFuture findAvailabilityByTag(String tenantId, String tag);

    ResultSetFuture insertData(AvailabilityMetric metric, int ttl);

    ResultSetFuture findAvailabilityData(AvailabilityMetric metric, long startTime, long endTime);

    ResultSetFuture updateCounter(Counter counter);

    ResultSetFuture updateCounters(Collection<Counter> counters);
}
