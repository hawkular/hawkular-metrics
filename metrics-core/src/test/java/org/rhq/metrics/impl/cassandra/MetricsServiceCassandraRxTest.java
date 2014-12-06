package org.rhq.metrics.impl.cassandra;

import static java.util.Arrays.asList;
import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ListenableFuture;

import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.NumericMetric2;
import org.rhq.metrics.core.Tenant;
import org.rhq.metrics.test.MetricsTest;

import rx.Observable;
import rx.Subscriber;

/**
 * @author John Sanda
 */
public class MetricsServiceCassandraRxTest extends MetricsTest {

    private MetricsServiceCassandraRx metricsService;

    @BeforeClass
    public void initClass() {
        initSession();
        DataAccess dataAccess = new DataAccess(session);
        metricsService = new MetricsServiceCassandraRx(dataAccess);
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE data");
        session.execute("TRUNCATE tags");
        session.executeAsync("TRUNCATE metrics_idx");
    }

    @Test
    public void addDataForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenantId)));

        NumericMetric2 m1 = new NumericMetric2(tenantId, new MetricId("m1"));
        m1.addData(start.plusSeconds(30).getMillis(), 11.2);
        m1.addData(start.getMillis(), 11.1);

        NumericMetric2 m2 = new NumericMetric2(tenantId, new MetricId("m2"));
        m2.addData(start.plusSeconds(30).getMillis(), 12.2);
        m2.addData(start.getMillis(), 12.1);

        NumericMetric2 m3 = new NumericMetric2(tenantId, new MetricId("m3"));

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        Observable<NumericMetric2> observable = metricsService.findNumericData(m1, start.getMillis(), end.getMillis());
        assertMetricEquals(single(observable), m1);

        observable = metricsService.findNumericData(m2, start.getMillis(), end.getMillis());
        assertMetricEquals(single(observable), m2);

        observable = metricsService.findNumericData(m3, start.getMillis(), end.getMillis());
        assertMetricEquals(single(observable), new NumericMetric2("test-tenant", new MetricId("m3")));
    }

    private NumericMetric2 single(Observable<NumericMetric2> observable) throws Exception {
        final AtomicReference<NumericMetric2> reference = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        observable.single().subscribe(new Subscriber<NumericMetric2>() {
            @Override
            public void onCompleted() {
                latch.countDown();
            }

            @Override
            public void onError(Throwable throwable) {
                error.set(throwable);
                latch.countDown();
            }

            @Override
            public void onNext(NumericMetric2 numericMetric2) {
                reference.set(numericMetric2);
            }
        });
        latch.await();
        if (error.get() != null) {
            fail("Failed to retrieve metric", error.get());
        }
        return reference.get();
    }

    private void assertMetricEquals(Metric actual, Metric expected) {
        assertEquals(actual, expected, "The metric doe not match the expected value");
        assertEquals(actual.getData(), expected.getData(), "The data does not match the expected values");
    }

}
