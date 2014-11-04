package org.rhq.metrics.core;

import static org.testng.Assert.assertEquals;

import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.metrics.test.MetricsTest;

/**
 * @author John Sanda
 */
public class DataAccessTest extends MetricsTest {

    private DataAccess dataAccess;

    @BeforeClass
    public void initClass() {
        initSession();
        dataAccess = new DataAccess(session);
    }

    @BeforeMethod
    public void initMethod() {
        resetDB();
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

        @SuppressWarnings("unchecked")
	ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insert1Future, insert2Future,
            insert3Future);

        getUninterruptibly(insertsFuture);

        ResultSetFuture queryFuture = dataAccess.findData("raw", metricId, hour(4).getMillis(),
            hour(4).plusMinutes(12).getMillis());

        ResultSet resultSet = getUninterruptibly(queryFuture);
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

//    @Test
//    public void updateCounter() throws Exception {
//        Counter expected = new Counter("simple-test", "c1", 1);
//
//        ResultSetFuture future = dataAccess.updateCounter(expected);
//        getUninterruptibly(future);
//
//        ResultSet resultSet = session.execute("SELECT * FROM counters WHERE group = '" + expected.getGroup() + "'");
//        List<Row> rows = resultSet.all();
//
//        assertEquals(rows.size(), 1, "Expected to get back 1 row");
//        assertEquals(toCounter(rows.get(0)), expected, "The returned counter does not match the expected value");
//    }

//    @Test
//    public void updateCounters() throws Exception {
//        String group = "batch-test";
//        List<Counter> expected = ImmutableList.of(
//            new Counter(group, "c1", 1),
//            new Counter(group, "c2", 2),
//            new Counter(group, "c3", 3)
//        );
//
//        ResultSetFuture future = dataAccess.updateCounters(expected);
//        getUninterruptibly(future);
//
//        ResultSet resultSet = session.execute("SELECT * FROM counters WHERE group = '" + group + "'");
//        List<Counter> actual = toCounters(resultSet);
//
//        assertEquals(actual, expected, "The counters do not match the expected values");
//    }
//
//    @Test
//    public void findCountersByGroup() throws Exception {
//        Counter c1 = new Counter("group1", "c1", 1);
//        Counter c2 = new Counter("group1", "c2", 2);
//        Counter c3 = new Counter("group2", "c1", 1);
//        Counter c4 = new Counter("group2", "c2", 2);
//
//        ResultSetFuture future = dataAccess.updateCounters(asList(c1, c2, c3, c4));
//        getUninterruptibly(future);
//
//        ResultSetFuture queryFuture = dataAccess.findCounters(c1.getGroup());
//        ResultSet resultSet = getUninterruptibly(queryFuture);
//        List<Counter> actual = toCounters(resultSet);
//
//        List<Counter> expected = asList(c1, c2);
//
//        assertEquals(actual, expected, "The counters do not match the expected values when filtering by group");
//    }
//
//    @Test
//    public void findCountersByGroupAndName() throws Exception {
//        String group = "batch-test";
//        Counter c1 = new Counter(group, "c1", 1);
//        Counter c2 = new Counter(group, "c2", 2);
//        Counter c3 = new Counter(group, "c3", 3);
//
//        ResultSetFuture future = dataAccess.updateCounters(asList(c1, c2, c3));
//        getUninterruptibly(future);
//
//        ResultSetFuture queryFuture = dataAccess.findCounters(group, asList("c1", "c3"));
//        ResultSet resultSet = getUninterruptibly(queryFuture);
//        List<Counter> actual = toCounters(resultSet);
//
//        List<Counter> expected = asList(c1, c3);
//
//        assertEquals(actual, expected,
//            "The counters do not match the expected values when filtering by group and by counter names");
//    }
//
//    private List<Counter> toCounters(ResultSet resultSet) {
//        List<Counter> counters = new ArrayList<>();
//        for (Row row : resultSet) {
//            counters.add(toCounter(row));
//        }
//        return counters;
//    }
//
//    private Counter toCounter(Row row) {
//        return new Counter(row.getString(0), row.getString(1), row.getLong(2));
//    }

}
