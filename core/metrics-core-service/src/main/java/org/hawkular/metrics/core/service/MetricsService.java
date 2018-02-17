/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.service;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.hawkular.metrics.model.AvailabilityBucketPoint;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NamedDataPoint;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.TaggedBucketPoint;
import org.hawkular.metrics.model.Tenant;
import org.hawkular.metrics.model.exception.MetricAlreadyExistsException;
import org.hawkular.metrics.model.param.BucketConfig;

import rx.Completable;
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
     * All data is associated with a {@link Tenant tenant} via the tenant id; however, the
     * foreign key like relationship is not enforced. Data can be inserted with a non-existent tenant id. More
     * importantly, data could be inserted with a tenant id that already exists.
     * </p>
     *
     * @param tenant
     *            The {@link Tenant tenant} to create
     * @param overwrite Flag to force overwrite previous tenant definition if it exists
     * @return void
     * @throws org.hawkular.metrics.model.exception.TenantAlreadyExistsException tenant already exists
     */
    Observable<Void> createTenant(Tenant tenant, boolean overwrite);

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
     * @param overwrite Flag to force overwrite previous metric definition if it exists
     *
     * @return This method only has side effects and does not return any data. As such,
     * {@link rx.Observer#onNext(Object) onNext} is not called. {@link rx.Observer#onCompleted()}  onCompleted}
     * is called when the operation completes successfully, and {@link rx.Observer#onError(Throwable)}  onError}
     * is called when it fails.
     */
    Observable<Void> createMetric(Metric<?> metric, boolean overwrite);

    Observable<MetricId<?>> findAllMetricIdentifiers();

    <T> Observable<Metric<T>> findMetric(MetricId<T> id);

    <T> Observable<Void> deleteMetric(MetricId<T> id);

    /**
     * Returns tenant's metric definitions. The results can be filtered using a type.
     *
     * @param type If type is null, all user definable metric definitions are returned.
     */
    <T> Observable<Metric<T>> findMetrics(String tenantId, MetricType<T> type);

    /**
     * Find tenant's metrics with filtering abilities. The filtering can take place at the type level or at the
     * tag level.
     *
     * The following tags-filtering capabilities are provided in tagsQueries:
     *
     * key: tagName ; value: *                -> Find all metrics with tag tagName and any value
     * key: tagName ; value: tagValue         -> Find all metrics with tag tagName and having value tagValue
     * key: tagName ; value: t1|t2|..         -> Find all metrics with tag tagName and having any of the values
     *                                           t1 or t2 etc
     *
     * @param tenantId The id of the tenant to which the metrics belong
     * @param type If type is null, no type filtering is used
     * @param tagsQuery If tagsQueries is empty, empty Observable is returned, use findMetrics(tenantId, type) instead
     * @return MetricIds that are filtered with given conditions
     */
    <T> Observable<MetricId<T>> findMetricIdentifiersWithFilters(String tenantId, MetricType<T> type, String tags);

    /**
     * Returns distinct tag values for a given tag query (using the same query format as {@link
     * #findMetricsWithFilters(String, MetricType, Map)}).
     *
     * @param tenantId The id of the tenant to which the metrics belong
     * @param metricType If type is null, no type filtering is used (values are merged)
     * @param tagsQueries If tagsQueries is empty, empty Observable is returned
     * @return A map with key as the tagname and set of possible values restricted by the query
     */
    Observable<Map<String, Set<String>>> getTagValues(String tenantId, MetricType<?> metricType,
                                                             Map<String, String> tagsQueries);

    Observable<Map<String, String>> getMetricTags(MetricId<?> id);

    Observable<String> getTagNames(String tenantId, MetricType<?> metricType, String filter);

    Observable<Void> addTags(Metric<?> metric, Map<String, String> tags);

    Observable<Void> deleteTags(Metric<?> metric, Set<String> tags);

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
     * @param limit      limit the number of data points
     * @param order      the sort order for the results, ASC if null
     *
     * @return an {@link Observable} that emits {@link DataPoint data points}
     */
    <T> Observable<DataPoint<T>> findDataPoints(MetricId<T> id, long start, long end, int limit, Order order);

    <T> Observable<DataPoint<T>> findDataPoints(MetricId<T> id, long start, long end, int limit, Order order,
            int pageSize);

    Completable verifyAndCreateTempTables(ZonedDateTime startTime, ZonedDateTime endTime);

    /**
     *
     * @param startTimeSlice
     * @param pageSize
     * @param maxConcurrency How many reads are concurrently called from Cassandra
     * @return
     */
    @SuppressWarnings("unchecked") Completable compressBlock(long startTimeSlice, int pageSize, int maxConcurrency);

    /**
     * Compresses the given range between timestamps to a single block.
     *
     * @param metrics Compressed metric definitions
     * @param startTimeSlice Start time of the block
     * @param endTimeSlice End time of the block
     * @param pageSize Cassandra query parameter
     * @return onComplete when job is done
     */
    Completable compressBlock(Observable<? extends MetricId<?>> metrics, long startTimeSlice, long endTimeSlice,
            int pageSize);

    <T> Observable<NamedDataPoint<T>> findDataPoints(List<MetricId<T>> ids, long start, long end, int limit,
                                                     Order order);

    /**
     * Fetch data points for multiple metrics searched by tag.
     *
     * @param tenantId The id of the tenant to which the metrics belong
     * @param metricType The type of the metrics
     * @param tagFilters The metric tag filter used to query for metrics
     * @param start      start time inclusive as a Unix timestamp in milliseconds
     * @param end        end time exclusive as a Unix timestamp in milliseconds
     * @param limit      limit the number of data points
     * @param order      the sort order for the results, ASC if null
     *
     * @return an {@link Observable} that emits {@link NamedDataPoint named data points}
     */
    <T> Observable<NamedDataPoint<T>> findDataPoints(String tenantId, MetricType<T> metricType,
            String tagFilters, long start, long end, int limit, Order order);

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

    Observable<List<NumericBucketPoint>> findGaugeStats(MetricId<Double> metricId, BucketConfig bucketConfig,
            List<Percentile> percentiles);

    /**
     * Queries data points from a single metric and groups results using tag filter expressions that are applied
     * against the tags of the each data point. The groups or buckets are then aggregated into "bucketed" data points
     * that consist of various statistics.
     *
     * @param metricId The gauge metric name
     * @param tags The tag filter expressions that define the grouping.
     * @param start The start time inclusive as a unix timestamp in milliseconds
     * @param end The end time exclusive as a unix timestamp in milliseconds
     * @param percentiles A list of percentiles to compute
     * @return A map of {@link TaggedBucketPoint tagged bucket points} that are keyed by a concatenation of tags of
     * the form tag_name:tag_value,tag_name:tag_value.
     */
    Observable<Map<String, TaggedBucketPoint>> findGaugeStats(MetricId<Double> metricId, Map<String, String> tags,
            long start, long end, List<Percentile> percentiles);

    /**
     * Fetches data points from multiple metrics. Down sampling is performed such that data points from all matching
     * metrics will go into one of the buckets. Functions are then applied to each bucket to produce a single
     * {@link NumericBucketPoint} for each bucket.
     *
     * @param metrics The {@link MetricId} list of the gauge or counter metrics that will be queried
     * @param start The start time inclusive as a Unix timestamp in milliseconds
     * @param end The end time exclusive as a Unix timestamp in milliseconds
     * @param buckets Determines the number of data points to be returned and how data points will be grouped based
     *                on which time slice or bucket they fall into.
     * @param stacked Should we stack multiple metrics?
     * @param isRate Set true to retrieve data as rates
     * @return An {@link Observable} that emits a single list of {@link NumericBucketPoint}
     */
    <T extends Number> Observable<List<NumericBucketPoint>> findNumericStats(
            List<MetricId<T>> metrics, long start, long end, Buckets buckets, List<Percentile>
            percentiles, boolean stacked, boolean isRate);

    Observable<DataPoint<AvailabilityType>> findAvailabilityData(MetricId<AvailabilityType> id, long start, long end,
                                                                 boolean distinct, int limit, Order order);

    Observable<List<AvailabilityBucketPoint>> findAvailabilityStats(MetricId<AvailabilityType> metricId, long start,
                                                                    long end, Buckets buckets);

    Observable<DataPoint<String>> findStringData(MetricId<String> id, long start, long end, boolean distinct,
            int limit, Order order);

    /**
     * Computes stats on a counter.
     *
     * @param id      counter metric id
     * @param bucketConfig bucket configuration
     *
     * @return an {@link Observable} emitting a single {@link List} of {@link NumericBucketPoint}
     */
    Observable<List<NumericBucketPoint>> findCounterStats(MetricId<Long> id, BucketConfig bucketConfig,
                                                          List<Percentile> percentiles);

    /**
     * Queries data points from a single metric and groups results using tag filter expressions that are applied
     * against the tags of the each data point. The groups or buckets are then aggregated into "bucketed" data points
     * that consist of various statistics.
     *
     * @param metricId The counter metric name
     * @param tags The tag filter expressions that define the grouping.
     * @param start The start time inclusive as a unix timestamp in milliseconds
     * @param end The end time exclusive as a unix timestamp in milliseconds
     * @param percentiles A list of percentiles to compute
     * @return A map of {@link TaggedBucketPoint tagged bucket points} that are keyed by a concatenation of tags of
     * the form tag_name:tag_value,tag_name:tag_value.
     */
    Observable<Map<String, TaggedBucketPoint>> findCounterStats(MetricId<Long> metricId, Map<String, String> tags,
            long start, long end, List<Percentile> percentiles);

    /**
     * Fetches gauge or counter data points and calculates per-minute rates.
     * For {@link MetricType#COUNTER}, reset events are detected and values reported after a reset are filtered out
     * before doing calculations in order to avoid inaccurate rates.
     *
     * @param id    This is the id of the metric
     * @param start The start time which is inclusive
     * @param end   The end time which is exclusive
     * @param limit      limit the number of data points
     * @param order      the sort order for the results, ASC if null
     *
     * @return An Observable of {@link DataPoint data points} which are emitted in ascending order. In other words,
     * the most recent data is emitted first.
     */
    Observable<DataPoint<Double>> findRateData(MetricId<? extends Number> id, long start, long end, int limit,
                                               Order order);

    <T extends Number> Observable<NamedDataPoint<Double>> findRateData(List<MetricId<T>> ids, long start, long end,
            int limit, Order order);

    /**
     * Computes stats on a counter or gauge rate.
     *
     * @param id      metric id
     * @param bucketConfig bucket configuration
     *
     * @return an {@link Observable} emitting a single {@link List} of {@link NumericBucketPoint}
     */
    Observable<List<NumericBucketPoint>> findRateStats(MetricId<? extends Number> id, BucketConfig bucketConfig,
                                                       List<Percentile> percentiles);

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
    <T> Func1<MetricId<T>, Boolean> idFilter(String regexp);
}
