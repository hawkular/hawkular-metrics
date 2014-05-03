package org.rhq.metrics.core;

import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;

import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.metrics.impl.cassandra.MetricsServiceCassandra;

/**
 * @author John Sanda
 */
public class MetricsServiceTest {

    private static final long FUTURE_TIMEOUT = 3;

    private MetricsServiceCassandra metricsService;

    private Session session;

    private DataAccess dataAccess;

    private RawMetricMapper rawMapper = new RawMetricMapper();

    @BeforeClass
    public void initClass() {
        Cluster cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("rhq");
        dataAccess = new DataAccess(session);

        metricsService = new MetricsServiceCassandra();
        metricsService.setDataAccess(dataAccess);
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE metrics");
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
        ResultSetFuture future = metricsService.findData("raw", "1", hour(5).plusMinutes(3).getMillis(),
            hour(5).plusMinutes(4).getMillis());

        ResultSet resultSet = getUniterruptibly(future);

        List<RawNumericMetric> actual = rawMapper.map(resultSet);

        assertEquals(actual.size(), expected.size(), "Expected to get back 3 raw metrics");
        assertEquals(actual, expected, "The returned raw metrics do not match the expected values");
    }

    private DateTime hour0() {
        DateTime rightNow = now();
        return rightNow.hourOfDay().roundFloorCopy().minusHours(
            rightNow.hourOfDay().roundFloorCopy().hourOfDay().get());
    }

    private DateTime hour(int hourOfDay) {
        return hour0().plusHours(hourOfDay);
    }

    private <V> V getUniterruptibly(Future<V> future) throws ExecutionException, TimeoutException {
        return Uninterruptibles.getUninterruptibly(future, FUTURE_TIMEOUT, TimeUnit.SECONDS);
    }

}
