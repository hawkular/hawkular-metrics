/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.core.impl;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import org.hawkular.metrics.core.api.AvailabilityBucketPoint;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.NumericBucketPoint;
import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.core.api.exception.MetricAlreadyExistsException;

import rx.Observable;
import rx.functions.Func1;

/**
 * Interface that defines the functionality of the Metrics Service.
 * @author Heiko W. Rupp
 */
public interface MetricsService {

    /**
     * <p>
     * This method should be call before ever inserting any data to ensure that the tenant id is unique and to establish
     * any global configuration for data retention and for pre-computed aggregates. An exception is thrown if a tenant
     * with the same id already exists.
     * </p>
     * <p>
     * All data is associated with a {@link org.hawkular.metrics.core.api.Tenant tenant} via the tenant id; however, the
     * foreign key like relationship is not enforced. Data can be inserted with a non-existent tenant id. More
     * importantly, data could be inserted with a tenant id that already exists.
     * </p>
     *
     * @param tenant
     *            The {@link Tenant tenant} to create
     * @return void
     * @throws org.hawkular.metrics.core.api.exception.TenantAlreadyExistsException
     *             tenant already exists
     */
    Observable<Void> createTenant(Tenant tenant);

    Observable<Tenant> getTenants();

    /**
     * <p>
     * Clients are not required to required to explicitly create a metric via this method before storing data for it.
     * This method does a few things. First, it updates indexes with the metric meta data (i.e., name, tags, etc.) so
     * that it can be found in metric queries performed with {@link #findMetrics(String, MetricType)}. Querying by
     * tags will be supported in the future. Secondly, this method ensures that there are no metric naming conflicts.
     * If another metric with the same name already exists, then returned Observable will fail with a
     * {@link MetricAlreadyExistsException}. Lastly, meta data settings are configured and persisted. Currently this
     * includes a couple things - data retention and counter rates. If data retention is specified for the metric,
     * those settings are stored so that they will be applied to any data points that get persisted. If the metric is
     * a counter, then a background job is created to compute and store rate data points. It is not yet possible to
     * configure the settings for the rate calculation job; however, that will change in the near future.
     * </p>
     * <p>
     * Note that in the current implementation if metric creation fails, things can be in an inconsistent state. For
     * example, an index that should have been updated might not have been. There is no work around for this currently.
     * </p>
     *
     * @param metric The metric to create
     *
     * @return This method only has side effects and does not return any data. As such,
     * {@link rx.Observer#onNext(Object) onNext} is not called. {@link rx.Observer#onCompleted()}  onCompleted}
     * is called when the operation completes successfully, and {@link rx.Observer#onError(Throwable)}  onError}
     * is called when it fails.
     */
    Observable<Void> createMetric(Metric<?> metric);

    <T> Observable<Metric<T>> findMetric(MetricId<T> id);

    /**
     * Returns tenant's metric definitions. The results can be filtered using a type.
     *
     * @param type If type is null, all user definable metric definitions are returned.
     */
    <T> Observable<Metric<T>> findMetrics(String tenantId, MetricType<T> type);

    /**
     * Find tenant's metrics with filtering abilities. The filtering can take place at the type level or at the
     * tag level. The following tags-filtering capabilities are provided in tagsQueries:
     *
     * key: tagName ; value: *                -> Find all metrics with tag tagName and any value
     * key: tagName ; value: tagValue         -> Find all metrics with tag tagName and having value tagValue
     * key: tagName ; value: t1|t2|..         -> Find all metrics with tag tagName and having any of the values
     *                                           t1 or t2 etc
     *
     * @param tenantId
     * @param tagsQueries If tagsQueries is empty, empty Observable is returned, use findMetrics(tenantId, type) instead
     * @param type If type is null, no type filtering is used
     * @return Metric's that are filtered with given conditions
     */
    <T> Observable<Metric<T>> findMetricsWithFilters(String tenantId, Map<String, String> tagsQueries,
            MetricType<T> type);

    Observable<Optional<Map<String, String>>> getMetricTags(MetricId<?> id);

    Observable<Void> addTags(Metric<?> metric, Map<String, String> tags);

    Observable<Void> deleteTags(Metric<?> metric, Map<String, String> tags);

    /**
     * Insert data points for the specified {@code metrics}.
     *
     *
     * @param metricType type of all metrics emitted by {@code metrics}
     * @param metrics the sources of data points
     *
     * @return an {@link Observable} emitting just one item on complete
     */
    <T> Observable<Void> addDataPoints(MetricType<T> metricType, Observable<Metric<T>> metrics);

    /**
     * Fetch data points for a single metric.
     *
     * @param id         identifier of the metric
     * @param start      start time inclusive as a Unix timestamp in milliseconds
     * @param end        end time exclusive as a Unix timestamp in milliseconds
     *
     * @return an {@link Observable} that emits {@link DataPoint data points}
     */
    <T> Observable<DataPoint<T>> findDataPoints(MetricId<T> id, long start, long end);

    /**
     * This method applies one or more functions to an Observable that emits data points of a gauge metric. The data
     * points Observable is asynchronous. The functions however, are applied serially in the order specified.
     *
     * @param id The metric name
     * @param start The start time inclusive as a Unix timestamp in milliseconds
     * @param end The end time exclusive as a Unix timestamp in milliseconds
     * @param funcs one or more functions to operate on the fetched gauge data
     * @return An {@link Observable} that emits the results with the same ordering as funcs
     * @see Aggregate
     */
    @SuppressWarnings("unchecked")
    <T> Observable<T> findGaugeData(MetricId<Double> id, long start, long end,
                                    Func1<Observable<DataPoint<Double>>, Observable<T>>... funcs);

    Observable<List<NumericBucketPoint>> findGaugeStats(MetricId<Double> metricId, long start, long end,
                                                        Buckets buckets, List<Double> percentiles);

    /**
     * Fetches data points from multiple metrics that are determined by a tags filter query. Down sampling is performed
     * such that data points from all matching metrics will go into one of the buckets. Functions are then applied to
     * each bucket to produce a single {@link NumericBucketPoint} for each bucket.
     *
     * @param tenantId The id of the tenant to which the metrics belong
     * @param tagFilters The metric tag filter used to query for metrics
     * @param start The start time inclusive as a Unix timestamp in milliseconds
     * @param end The end time exclusive as a Unix timestamp in milliseconds
     * @param buckets Determines the number of data points to be returned and how data points will be grouped based
     *                on which time slice or bucket they fall into.
     * @return An {@link Observable} that emits a single list of {@link NumericBucketPoint}
     */
    <T extends Number> Observable<List<NumericBucketPoint>> findNumericStats(String tenantId, MetricType<T> metricType,
            Map<String, String> tagFilters, long start, long end, Buckets buckets, List<Double> percentiles,
            boolean stacked);

    /**
     * Fetches data points from multiple metrics. Down sampling is performed such that data points from all matching
     * metrics will go into one of the buckets. Functions are then applied to each bucket to produce a single
     * {@link NumericBucketPoint} for each bucket.
     *
     * @param tenantId The id of the tenant to which the metrics belong
     * @param metrics The names of the gauge metrics that will be queried
     * @param start The start time inclusive as a Unix timestamp in milliseconds
     * @param end The end time exclusive as a Unix timestamp in milliseconds
     * @param buckets Determines the number of data points to be returned and how data points will be grouped based
     *                on which time slice or bucket they fall into.
     * @return An {@link Observable} that emits a single list of {@link NumericBucketPoint}
     */
    <T extends Number> Observable<List<NumericBucketPoint>> findNumericStats(String tenantId, MetricType<T> metricType,
            List<String> metrics, long start, long end, Buckets buckets, List<Double> percentiles, boolean stacked);

    Observable<DataPoint<AvailabilityType>> findAvailabilityData(MetricId<AvailabilityType> id, long start, long end,
                                                                 boolean distinct);

    Observable<List<AvailabilityBucketPoint>> findAvailabilityStats(MetricId<AvailabilityType> metricId, long start,
                                                                    long end, Buckets buckets);

    /**
     * Check if a metric has been stored in the system.
     */
    Observable<Boolean> idExists(MetricId<?> metric);

    /**
     * Computes stats on a counter.
     *
     * @param id      counter metric id
     * @param start   start time, inclusive
     * @param end     end time, exclusive
     * @param buckets bucket configuration
     *
     * @return an {@link Observable} emitting a single {@link List} of {@link NumericBucketPoint}
     */
    Observable<List<NumericBucketPoint>> findCounterStats(MetricId<Long> id, long start, long end, Buckets buckets,
                                                          List<Double> percentiles);

    /**
     * Fetches counter data points and calculates per-minute rates. Resets events are detected and values reported
     * after a reset are filtered out before doing calculations in order to avoid inaccurate rates.
     *
     * @param id This is the id of the counter metric
     * @param start The start time which is inclusive
     * @param end The end time which is exclusive
     *
     * @return An Observable of {@link DataPoint data points} which are emitted in ascending order. In other words,
     * the most recent data is emitted first.
     */
    Observable<DataPoint<Double>> findRateData(MetricId<Long> id, long start, long end);

    /**
     * Computes stats on a counter rate.
     *
     * @param id      counter metric id
     * @param start   start time, inclusive
     * @param end     end time, exclusive
     * @param buckets bucket configuration
     *
     * @return an {@link Observable} emitting a single {@link List} of {@link NumericBucketPoint}
     */
    Observable<List<NumericBucketPoint>> findRateStats(MetricId<Long> id, long start, long end, Buckets buckets,
                                                       List<Double> percentiles);

    /**
     * <p>
     * For a specified date range, return a list of periods in which the predicate evaluates to true for each
     * consecutive data point. The periods are returned in ascending order. Consider the following data points,
     * </p>
     * <p>
     * {time: 1, value: 5}, {time: 2, value: 11}, {time: 3, value: 12}, {time: 4, value: 8}, {time: 5, value: 14},
     * {time: 6, value: 7}, {time: 7, value: 16}
     * </p>
     *<p>
     * And a predicate that tests for values greater than 10. The results would be,
     *</p>
     * <p>
     * {start: 2, end: 3}, {start: 5, end: 5}, {start: 7, end: 7}
     * </p>
     *
     * @param predicate A function applied to the value of each data point
     * @param start The start time inclusive
     * @param end The end time exclusive
     * @return Each element in the list is a two element array. The first element is the start time inclusive for which
     * the predicate matches, and the second element is the end time inclusive for which the predicate matches.
     */
    Observable<List<long[]>> getPeriods(MetricId<Double> id, Predicate<Double> predicate, long start, long end);

    /**
     * @return a hot {@link Observable} emitting {@link Metric} events after data has been inserted
     */
    Observable<Metric<?>> insertedDataEvents();
}
