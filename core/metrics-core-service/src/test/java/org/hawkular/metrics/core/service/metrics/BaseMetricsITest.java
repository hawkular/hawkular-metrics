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
package org.hawkular.metrics.core.service.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.hawkular.metrics.core.service.BaseITest;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.DataAccessImpl;
import org.hawkular.metrics.core.service.DataRetentionsMapper;
import org.hawkular.metrics.core.service.DelegatingDataAccess;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.core.service.PercentileWrapper;
import org.hawkular.metrics.core.service.TestDataAccessFactory;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Retention;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author John Sanda
 */
public abstract class BaseMetricsITest extends BaseITest {

    protected static final int DEFAULT_TTL = 7; //days

    protected static final int COMPRESSION_PAGE_SIZE = 1000;

    protected MetricsServiceImpl metricsService;

    protected DataAccess dataAccess;

    private Function<Double, PercentileWrapper> defaultCreatePercentile;

    @BeforeClass(alwaysRun = true)
    public void initClass() {
        this.dataAccess = TestDataAccessFactory.newInstance(session);

        ConfigurationService configurationService = new ConfigurationService() ;
        configurationService.init(rxSession);

        defaultCreatePercentile = NumericDataPointCollector.createPercentile;

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(this.dataAccess);
        metricsService.setConfigurationService(configurationService);
        metricsService.setDefaultTTL(DEFAULT_TTL);
        metricsService.startUp(session, getKeyspace(), true, metricRegistry);
    }

    @BeforeMethod(alwaysRun = true)
    public void initMethod() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE data");
        session.execute(String.format("TRUNCATE %s", DataAccessImpl.OUT_OF_ORDER_TABLE_NAME));
        session.execute("TRUNCATE data_compressed");
        session.execute("TRUNCATE metrics_idx");
        session.execute("TRUNCATE retentions_idx");
        session.execute("TRUNCATE metrics_tags_idx");
        session.execute("TRUNCATE leases");

        // Need to truncate all the temp tables also..
        for (TableMetadata tableMetadata : session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace())
                .getTables()) {
            if(tableMetadata.getName().startsWith(DataAccessImpl.TEMP_TABLE_NAME_PROTOTYPE)) {
                session.execute(String.format("TRUNCATE %s", tableMetadata.getName()));
            }
        }

        NumericDataPointCollector.createPercentile = defaultCreatePercentile;
    }

    @AfterClass(alwaysRun = true)
    public void shutdown() {
        dataAccess.shutdown();
    }

    protected <T extends Number> NumericBucketPoint createSingleBucket(List<? extends DataPoint<T>> combinedData,
            DateTime start, DateTime end) {
        T expectedMin = combinedData.stream()
                .min((x, y) -> Double.compare(x.getValue().doubleValue(), y.getValue().doubleValue()))
                .get()
                .getValue();
        T expectedMax = combinedData.stream()
                .max((x, y) -> Double.compare(x.getValue().doubleValue(), y.getValue().doubleValue()))
                .get()
                .getValue();
        PercentileWrapper expectedMedian = NumericDataPointCollector.createPercentile.apply(50.0);
        Mean expectedAverage = new Mean();
        Sum expectedSamples = new Sum();
        Sum expectedSum = new Sum();
        combinedData.stream().forEach(arg -> {
            expectedMedian.addValue(arg.getValue().doubleValue());
            expectedAverage.increment(arg.getValue().doubleValue());
            expectedSamples.increment(1);
            expectedSum.increment(arg.getValue().doubleValue());
        });

        return new NumericBucketPoint.Builder(start.getMillis(), end.getMillis())
                .setMin(expectedMin.doubleValue())
                .setMax(expectedMax.doubleValue())
                .setAvg(expectedAverage.getResult())
                .setMedian(expectedMedian.getResult())
                .setSum(expectedSum.getResult())
                .setSamples(new Double(expectedSamples.getResult()).intValue())
                .build();
    }

    protected static void assertNumericBucketsEquals(List<NumericBucketPoint> actual,
            List<NumericBucketPoint> expected) {
        String msg = String.format("%nExpected:%n%s%nActual:%n%s%n", expected, actual);
        assertEquals(actual.size(), expected.size(), msg);
        IntStream.range(0, actual.size()).forEach(i -> {
            NumericBucketPoint actualPoint = actual.get(i);
            NumericBucketPoint expectedPoint = expected.get(i);
            assertNumericBucketEquals(actualPoint, expectedPoint, msg);
        });
    }

    protected static void assertNumericBucketEquals(NumericBucketPoint actual, NumericBucketPoint expected,
            String msg) {
        assertEquals(actual.getStart(), expected.getStart(), msg);
        assertEquals(actual.getEnd(), expected.getEnd(), msg);
        assertEquals(actual.isEmpty(), expected.isEmpty(), msg);
        if (!actual.isEmpty()) {
            assertEquals(actual.getAvg(), expected.getAvg(), 0.001, msg);
            assertEquals(actual.getMax(), expected.getMax(), 0.001, msg);
            assertEquals(actual.getMedian(), expected.getMedian(), 0.001, msg);
            assertEquals(actual.getMin(), expected.getMin(), 0.001, msg);
            assertEquals(actual.getSum(), expected.getSum(), 0.001, msg);
            assertEquals(actual.getSamples(), expected.getSamples(), 0, msg);
            for(int i = 0; i < expected.getPercentiles().size(); i++) {
                assertEquals(actual.getPercentiles().get(i).getOriginalQuantile(),
                        expected.getPercentiles().get(i).getOriginalQuantile(),  msg);
                assertEquals(actual.getPercentiles().get(i).getValue(), expected.getPercentiles().get(i).getValue(),
                        0.001, msg);
            }
        }
    }

    protected void assertMetricIndexMatches(String tenantId, MetricType<?> type, List<Metric<?>> expected)
        throws Exception {
        Set<Metric<?>> actualIndex = Sets
                .newHashSet(metricsService.findMetrics(tenantId, type).doOnError(Throwable::printStackTrace).toBlocking().toIterable());
        assertEquals(actualIndex, Sets.newHashSet(expected), "The metrics index results do not match");
    }

    protected class MetricsTagsIndexEntry {
        String tagValue;
        MetricId<?> id;

        public MetricsTagsIndexEntry(String tagValue, MetricId<?> id) {
            this.tagValue = tagValue;
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MetricsTagsIndexEntry that = (MetricsTagsIndexEntry) o;

            if (!id.equals(that.id)) return false;
            if (!tagValue.equals(that.tagValue)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = tagValue.hashCode();
            result = 31 * result + id.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("tagValue", tagValue)
                .add("id", id)
                .toString();
        }
    }

    protected void assertMetricsTagsIndexMatches(String tenantId, String tag, List<MetricsTagsIndexEntry> expected)
        throws Exception {
        List<Row> rows = dataAccess.findMetricsByTagName(tenantId, tag).toList().toBlocking().first();
        List<MetricsTagsIndexEntry> actual = new ArrayList<>();

        for (Row row : rows) {
            MetricType<?> type = MetricType.fromCode(row.getByte(1));
            MetricId<?> id = new MetricId<>(tenantId, type, row.getString(2));
            actual.add(new MetricsTagsIndexEntry(row.getString(3), id)); // Need value here.. pff.
        }

        assertEquals(actual, expected, "The metrics tags index entries do not match");
    }

    protected void assertDataRetentionsIndexContains(String tenantId, MetricType<?> type, Set<Retention> expected)
        throws Exception {
        ResultSetFuture queryFuture = dataAccess.findDataRetentions(tenantId, type);
        ListenableFuture<Set<Retention>> retentionsFuture = Futures.transform(queryFuture,
                new DataRetentionsMapper(tenantId, type));
        Set<Retention> actual = getUninterruptibly(retentionsFuture);

        assertTrue(actual.containsAll(expected), "Expected data retentions index to contain " + expected +
                " but found " + actual);
    }

    protected void assertDataRetentionsIndexMatches(String tenantId, MetricType<?> type, Set<Retention> expected)
        throws Exception {
        ResultSetFuture queryFuture = dataAccess.findDataRetentions(tenantId, type);
        ListenableFuture<Set<Retention>> retentionsFuture = Futures.transform(queryFuture,
                new DataRetentionsMapper(tenantId, type));
        Set<Retention> actual = getUninterruptibly(retentionsFuture);

        assertEquals(actual, expected, "The data retentions are wrong");
    }

    protected static class VerifyTTLDataAccess extends DelegatingDataAccess {

        private int gaugeTTL;

        private int availabilityTTL;

        public VerifyTTLDataAccess(DataAccess instance) {
            super(instance);
            gaugeTTL = DEFAULT_TTL;
            availabilityTTL = DEFAULT_TTL;
        }

        public void setGaugeTTL(int expectedTTL) {
            this.gaugeTTL = expectedTTL;
        }

        public void setAvailabilityTTL(int availabilityTTL) {
            this.availabilityTTL = availabilityTTL;
        }

        // TODO We should only test the data_compressed TTL setting
//        @Override
//        public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> metric, int ttl) {
//            assertEquals(ttl, availabilityTTL, "The availability data TTL does not match the expected value when " +
//                "inserting data");
//            return super.insertAvailabilityData(metric, ttl);
//        }
    }

    protected static class InMemoryPercentileWrapper implements PercentileWrapper {
        List<Double> values = new ArrayList<>();
        double percentile;

        public InMemoryPercentileWrapper(double percentile) {
            this.percentile = percentile;
        }

        @Override public void addValue(double value) {
            values.add(value);
        }

        @Override public double getResult() {
            org.apache.commons.math3.stat.descriptive.rank.Percentile percentileCalculator =
                    new org.apache.commons.math3.stat.descriptive.rank.Percentile(percentile);
            double[] array = new double[values.size()];
            for (int i = 0; i < array.length; ++i) {
                array[i] = values.get(i++);
            }
            percentileCalculator.setData(array);

            return percentileCalculator.getQuantile();
        }
    }

    protected String createRandomId() {
        return UUID.randomUUID().toString();
    }
}
