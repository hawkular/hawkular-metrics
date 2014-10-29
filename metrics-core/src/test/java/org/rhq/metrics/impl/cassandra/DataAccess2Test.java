package org.rhq.metrics.impl.cassandra;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;

import org.joda.time.Days;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.metrics.core.AggregationTemplate;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.Tenant;
import org.rhq.metrics.test.MetricsTest;

/**
 * @author John Sanda
 */
public class DataAccess2Test extends MetricsTest {

    private DataAccess2 dataAccess;

    private PreparedStatement truncateTenants;

    private PreparedStatement truncateNumericData;

    @BeforeClass
    public void initClass() {
        initSession();
        dataAccess = new DataAccess2(session);
        truncateTenants = session.prepare("TRUNCATE tenants");
        truncateNumericData = session.prepare("TRUNCATE numeric_data");
    }

    @BeforeMethod
    public void initMethod() {
        session.execute(truncateTenants.bind());
        session.execute(truncateNumericData.bind());
    }

    @Test
    public void insertAndFindTenant() throws Exception {
        Tenant tenant1 = new Tenant().setId("tenant-1")
            .addAggregationTemplate(new AggregationTemplate()
                .setType(MetricType.NUMERIC)
                .setInterval(new Interval(5, Interval.Units.MINUTES))
                .setFunctions(ImmutableSet.of("max", "min", "avg")))
            .setRetention(MetricType.NUMERIC, Days.days(31).toStandardHours().getHours())
            .setRetention(MetricType.NUMERIC, new Interval(5, Interval.Units.MINUTES),
                Days.days(100).toStandardHours().getHours());

        Tenant tenant2 = new Tenant().setId("tenant-2")
            .setRetention(MetricType.NUMERIC, Days.days(14).toStandardHours().getHours())
            .addAggregationTemplate(new AggregationTemplate()
                .setType(MetricType.NUMERIC)
                .setInterval(new Interval(5, Interval.Units.HOURS))
                .setFunctions(ImmutableSet.of("sum", "count")));


        ResultSetFuture insertFuture = dataAccess.insertTenant(tenant1);
        getUninterruptibly(insertFuture);

        insertFuture = dataAccess.insertTenant(tenant2);
        getUninterruptibly(insertFuture);

        Set<Tenant> actual = dataAccess.findTenants();
        Set<Tenant> expected = ImmutableSet.of(tenant1, tenant2);

        assertEquals(actual, expected, "The tenants do not match");
    }

    @Test
    public void insertAndFindNumericRawData() throws Exception {
        NumericData d1 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimeUUID(UUIDs.timeBased())
            .setValue(1.23);

        NumericData d2 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimeUUID(UUIDs.timeBased())
            .setValue(1.234);

        NumericData d3 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-2")
            .setTimeUUID(UUIDs.timeBased())
            .setValue(1.234);

        ResultSetFuture insertFuture = dataAccess.insertNumericData(d1);
        getUninterruptibly(insertFuture);

        insertFuture = dataAccess.insertNumericData(d2);
        getUninterruptibly(insertFuture);

        insertFuture = dataAccess.insertNumericData(d3);
        getUninterruptibly(insertFuture);

        ResultSetFuture queryFuture = dataAccess.findNumericData(d1.getTenantId(), d1.getMetric(), Interval.NONE, 0L);
        ResultSet resultSet = getUninterruptibly(queryFuture);

        List<NumericData> expected = asList(d2, d1);
        List<NumericData> actual = new ArrayList<>();
        for (Row row : resultSet) {
            actual.add(new NumericData()
                .setTenantId(row.getString(0))
                .setMetric(row.getString(1))
                .setTimeUUID(row.getUUID(4))
                .setValue(row.getDouble(6)));
        }

        assertEquals(actual, expected, "The numeric data does not match");
    }

    @Test
    public void addNumericAttributes() throws Exception {
       ResultSetFuture insertFuture = dataAccess.addNumericAttribute("tenant-1", "metric-1", Interval.NONE, 0, "units",
           "KB");
       getUninterruptibly(insertFuture);

        insertFuture = dataAccess.addNumericAttribute("tenant-1", "metric-1", Interval.NONE, 0, "env", "test");
        getUninterruptibly(insertFuture);

        NumericData d1 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimeUUID(UUIDs.timeBased())
            .setValue(1.23);

        NumericData d2 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimeUUID(UUIDs.timeBased())
            .setValue(1.234);

        NumericData d3 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-2")
            .setTimeUUID(UUIDs.timeBased())
            .setValue(1.234);

        insertFuture = dataAccess.insertNumericData(d1);
        getUninterruptibly(insertFuture);

        insertFuture = dataAccess.insertNumericData(d2);
        getUninterruptibly(insertFuture);

        insertFuture = dataAccess.insertNumericData(d3);
        getUninterruptibly(insertFuture);

        ResultSetFuture queryFuture = dataAccess.findNumericData(d1.getTenantId(), d1.getMetric(), Interval.NONE, 0L);
        ResultSet resultSet = getUninterruptibly(queryFuture);

        List<NumericData> expected = asList(
            d2.putAttribute("units", "KB").putAttribute("env", "test"),
            d1.putAttribute("units", "KB").putAttribute("env", "test")
        );
        List<NumericData> actual = new ArrayList<>();
        for (Row row : resultSet) {
            actual.add(new NumericData()
                .setTenantId(row.getString(0))
                .setMetric(row.getString(1))
                .setTimeUUID(row.getUUID(4))
                .setValue(row.getDouble(6))
                .putAttributes(row.getMap(5, String.class, String.class)));
        }

        assertEquals(actual, expected, "The numeric data does not match");
    }

}
