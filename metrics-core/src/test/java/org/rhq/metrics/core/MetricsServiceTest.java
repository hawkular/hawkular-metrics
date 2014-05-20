package org.rhq.metrics.core;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.metrics.impl.cassandra.MetricsServiceCassandra;
import org.rhq.metrics.test.MetricsTest;

/**
 * @author John Sanda
 */
public class MetricsServiceTest extends MetricsTest {

    private MetricsServiceCassandra metricsService;

    private DataAccess dataAccess;

    private RawMetricMapper rawMapper = new RawMetricMapper();

    @BeforeClass
    public void initClass() {
        initSession();
        dataAccess = new DataAccess(session);

        metricsService = new MetricsServiceCassandra();
        metricsService.setDataAccess(dataAccess);
    }

    @BeforeMethod
    public void initMethod() {
        resetDB();
    }


    @Test
    public void addAndFetchRawData() throws Exception {
        Set<RawNumericMetric> data = ImmutableSet.of(
            new RawNumericMetric("1", 22.3, hour(5).plusMinutes(2).getMillis()),
            new RawNumericMetric("2", 10.234, hour(5).plusMinutes(2).plusSeconds(5).getMillis()),
            new RawNumericMetric("1", 21.9, hour(5).plusMinutes(3).plusSeconds(10).getMillis()),
            new RawNumericMetric("1", 23.32, hour(5).plusMinutes(3).plusSeconds(15).getMillis()),
            new RawNumericMetric("1", 23.32, hour(5).plusMinutes(4).getMillis())
        );

        Set<RawNumericMetric> expected = ImmutableSet.of(
            new RawNumericMetric("1", 21.9, hour(5).plusMinutes(3).plusSeconds(10).getMillis()),
            new RawNumericMetric("1", 23.32, hour(5).plusMinutes(3).plusSeconds(15).getMillis())
        );

        metricsService.addData(data);
        ListenableFuture<List<RawNumericMetric>> future = metricsService.findData("raw", "1",
            hour(5).plusMinutes(3).getMillis(), hour(5).plusMinutes(4).getMillis());

        List<RawNumericMetric> actual = getUninterruptibly(future);

        assertEquals(actual.size(), expected.size(), "Expected to get back 3 raw metrics");
        assertEquals(actual, expected, "The returned raw metrics do not match the expected values");
    }

}
