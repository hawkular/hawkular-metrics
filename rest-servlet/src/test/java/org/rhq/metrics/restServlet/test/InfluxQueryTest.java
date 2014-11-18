package org.rhq.metrics.restServlet.test;

import static org.junit.Assert.assertEquals;
import static org.rhq.metrics.core.MetricsService.DEFAULT_TENANT_ID;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.restServlet.influx.InfluxHandler;
import org.rhq.metrics.restServlet.influx.InfluxQuery;
import org.rhq.metrics.util.TimeUUIDUtils;

/**
 * Some testing of the Influx Query parser
 * @author Heiko W. Rupp
 */
public class InfluxQueryTest {

    @Test
    public void testQuantil() throws Exception {

        double[] input = {1,1,1,3,4,7,9,11,13,13};

        Metric metric = new Metric().setTenantId(DEFAULT_TENANT_ID).setId(new MetricId("influx-test"));
        UUID timeUUID = TimeUUIDUtils.getTimeUUID(new java.util.Date());
        List<NumericData> metrics = new ArrayList<>(10);
        for (Double val: input) {
            metrics.add(new NumericData(metric, timeUUID, val));
        }

        double q30 = new InfluxHandler().quantil(metrics,0.3*100);
        assertEquals(2, q30, 0.1);

        double q75 = new InfluxHandler().quantil(metrics,75);
        assertEquals(11, q75, 0.1);

        double q50 = new InfluxHandler().quantil(metrics,50);
        assertEquals(5.5, q50, 0.1);

    }

    @Test
    public void testInFlux1() throws Exception {

        String query = "select  derivative(value) from \"snert.bytes_out\" where  time > now() - 6h     group by time(5m)  order asc";

        InfluxQuery iq = new InfluxQuery(query);

        assertEquals("derivative", iq.getMapping());
        assertEquals("snert.bytes_out", iq.getMetric());


    }

    @Test
    public void testInflux2() throws Exception {
        String query = "select  mean(\"value\") as \"value_mean\" from \"snert.cpu_user\" where  time > now() - 6h     group by time(30s)  order asc";

        InfluxQuery iq = new InfluxQuery(query);

        assertEquals("mean", iq.getMapping());
        assertEquals("snert.cpu_user", iq.getMetric());
        assertEquals("value_mean",iq.getAlias());


    }

    @Test
    public void testInflux2days() throws Exception {
        String query = "select  mean(\"value\") as \"value_mean\" from \"snert.cpu_user\" where  time > now() - 2d     group by time(30s)  order asc";

        InfluxQuery iq = new InfluxQuery(query);

        assertEquals("mean", iq.getMapping());
        assertEquals("snert.cpu_user", iq.getMetric());
        assertEquals("value_mean",iq.getAlias());


    }

    @Test
    public void testInflux2days2() throws Exception {
        String query = "select  mean(\"value\") as \"value_mean\" from \"snert.cpu_user\" where  time > now() - 2d     group by time(30m)  order asc";

        InfluxQuery iq = new InfluxQuery(query);

        assertEquals("mean", iq.getMapping());
        assertEquals("snert.cpu_user", iq.getMetric());
        assertEquals("value_mean",iq.getAlias());


    }

    @Test
    public void testInflux3() throws Exception {
        String query = "select  mean(\"value\") as \"value_mean\" from \"snert.cpu_user\" where  time > 1402826660s and time < 1402934869s     group by time(1m)  order asc";

        InfluxQuery iq = new InfluxQuery(query);

        assertEquals("mean", iq.getMapping());
        assertEquals("snert.cpu_user", iq.getMetric());
        assertEquals(1402826660000l,iq.getStart());
        assertEquals(1402934869000l,iq.getEnd());
        assertEquals("value_mean",iq.getAlias());


    }

}
