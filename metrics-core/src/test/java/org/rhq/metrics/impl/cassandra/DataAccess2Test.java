package org.rhq.metrics.impl.cassandra;

import static org.testng.Assert.assertEquals;

import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.ImmutableSet;

import org.joda.time.Days;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.metrics.core.AggregationTemplate;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.Tenant;
import org.rhq.metrics.test.MetricsTest;

/**
 * @author John Sanda
 */
public class DataAccess2Test extends MetricsTest {

    private DataAccess2 dataAccess;

    private PreparedStatement truncateTenants;

    @BeforeClass
    public void initClass() {
        initSession();
        dataAccess = new DataAccess2(session);
        truncateTenants = session.prepare("TRUNCATE tenants");
    }

    @BeforeMethod
    public void initMethod() {
        session.execute(truncateTenants.bind());
    }

    @Test
    public void insertAndFindTenant() throws Exception {
        Tenant tenant1 = new Tenant().setId("tenant-1")
            .addAggregationTemplate(new AggregationTemplate()
                .setType(MetricType.NUMERIC)
                .setInterval("5min")
                .setFunctions(ImmutableSet.of("max", "min", "avg")))
            .setRetention(MetricType.NUMERIC, Days.days(31).toStandardHours().getHours());

        Tenant tenant2 = new Tenant().setId("tenant-2")
            .setRetention(MetricType.NUMERIC, Days.days(14).toStandardHours().getHours())
            .addAggregationTemplate(new AggregationTemplate()
                .setType(MetricType.NUMERIC)
                .setInterval("1hr")
                .setFunctions(ImmutableSet.of("sum", "count")));


        ResultSetFuture insertFuture = dataAccess.insertTenant(tenant1);
        getUninterruptibly(insertFuture);

        insertFuture = dataAccess.insertTenant(tenant2);
        getUninterruptibly(insertFuture);

        Set<Tenant> actual = dataAccess.findTenants();
        Set<Tenant> expected = ImmutableSet.of(tenant1, tenant2);

        assertEquals(actual, expected, "The tenants do not match");
    }

}
