package org.rhq.metrics.test;

import static org.joda.time.DateTime.now;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import org.joda.time.DateTime;

/**
 * @author John Sanda
 */
public class MetricsTest {

    private static final long FUTURE_TIMEOUT = 3;

    protected Session session;

    private PreparedStatement truncateMetrics;

    public void initSession() {
        String nodeAddresses = System.getProperty("nodes", "127.0.0.1");
        Cluster cluster = new Cluster.Builder().addContactPoints(nodeAddresses.split(",")).build();
        session = cluster.connect(getKeyspace());

        truncateMetrics = session.prepare("TRUNCATE metrics");
    }

    protected void resetDB() {
        session.execute(truncateMetrics.bind());
    }

    protected String getKeyspace() {
        return System.getProperty("keyspace", "rhqtest");
    }

    protected DateTime hour0() {
        DateTime rightNow = now();
        return rightNow.hourOfDay().roundFloorCopy().minusHours(
            rightNow.hourOfDay().roundFloorCopy().hourOfDay().get());
    }

    protected DateTime hour(int hourOfDay) {
        return hour0().plusHours(hourOfDay);
    }

    protected <V> V getUninterruptibly(ListenableFuture<V> future) throws ExecutionException, TimeoutException {
        return Uninterruptibles.getUninterruptibly(future, FUTURE_TIMEOUT, TimeUnit.SECONDS);
    }

}
