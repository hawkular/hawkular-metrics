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

}
