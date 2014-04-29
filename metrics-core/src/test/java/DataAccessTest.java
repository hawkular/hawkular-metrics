import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.rhq.metrics.core.DataAccess;
import org.rhq.metrics.core.DataType;

/**
 * @author John Sanda
 */
public class DataAccessTest {

    private static final long FUTURE_TIMEOUT = 3;

    private DataAccess dataAccess;

    private Session session;

    @BeforeClass
    public void initClass() {
        Cluster cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("rhq");
        dataAccess = new DataAccess(session);
    }

    @Test
    public void insertAndFindData() throws Exception {
        String metricId = "metric-id-1";
        int ttl = 360;

        ResultSetFuture insert1Future = dataAccess.insertData("raw", metricId, hour(4).plusSeconds(5).getMillis(),
            ImmutableMap.of(DataType.RAW.ordinal(), 1.23), ttl);
        ResultSetFuture insert2Future = dataAccess.insertData("raw", metricId, hour(4).plusMinutes(10).getMillis(),
            ImmutableMap.of(DataType.RAW.ordinal(), 1.29), ttl);
        ResultSetFuture insert3Future = dataAccess.insertData("raw", metricId, hour(4).plusMinutes(12).getMillis(),
            ImmutableMap.of(DataType.RAW.ordinal(), 1.57), ttl);

        ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insert1Future, insert2Future,
            insert3Future);

        getUniterruptibly(insertsFuture);

        ResultSetFuture queryFuture = dataAccess.findData("raw", metricId, hour(4).getMillis(),
            hour(4).plusMinutes(12).getMillis());

        ResultSet resultSet = getUniterruptibly(queryFuture);
        List<Row> rows = resultSet.all();

        assertEquals(rows.size(), 2, "Expected to get back two rows");

        assertEquals(rows.get(0).getString(0), metricId, "The metricId for row[0] is wrong");
        assertEquals(rows.get(0).getDate(1), hour(4).plusSeconds(5).toDate(), "The timestamp for row[0] is wrong");
        assertEquals(rows.get(0).getMap(2, Integer.class, Double.class), ImmutableMap.of(DataType.RAW.ordinal(), 1.23),
            "The value for row[0] is wrong");

        assertEquals(rows.get(1).getString(0), metricId, "The metricId for row[1] is wrong");
        assertEquals(rows.get(1).getDate(1), hour(4).plusMinutes(10).toDate(), "The timestamp for row[1] is wrong");
        assertEquals(rows.get(1).getMap(2, Integer.class, Double.class), ImmutableMap.of(DataType.RAW.ordinal(), 1.29),
            "The value for row[1] is wrong");
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
