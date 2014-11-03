package org.rhq.metrics.core;

import static java.util.Arrays.asList;
import static org.joda.time.DateTime.now;
import static org.rhq.metrics.util.TimeUUIDUtils.getTimeUUID;
import static org.testng.Assert.assertEquals;

import java.util.List;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import org.joda.time.DateTime;
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

    @BeforeClass
    public void initClass() {
        initSession();
        metricsService = new MetricsServiceCassandra();
        metricsService.startUp(session);
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE numeric_data");
    }


    @Test
    public void addAndFetchRawData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "t1";
        String metric = "m1";

        NumericData d1 = new NumericData().setTenantId(tenantId).setMetric("m1").setTimeUUID(getTimeUUID(start))
            .setValue(1.1);
        NumericData d2 = new NumericData().setTenantId(tenantId).setMetric("m1")
            .setTimeUUID(getTimeUUID(start.plusMinutes(2))).setValue(2.2);
        NumericData d3 = new NumericData().setTenantId(tenantId).setMetric("m1")
            .setTimeUUID(getTimeUUID(start.plusMinutes(4))).setValue(3.3);
        NumericData d4 = new NumericData().setTenantId(tenantId).setMetric("m1").setTimeUUID(getTimeUUID(end))
            .setValue(4.4);

        ListenableFuture<Void> insertFuture =  metricsService.addNumericData(ImmutableSet.of(d1, d2, d3, d4));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> queryFuture = metricsService.findData(tenantId, metric, start.getMillis(),
            end.getMillis());
        List<NumericData> actual = getUninterruptibly(queryFuture);
        List<NumericData> expected = asList(d3, d2, d1);

        assertEquals(actual, expected, "The numeric raw data does not match");
    }

}
