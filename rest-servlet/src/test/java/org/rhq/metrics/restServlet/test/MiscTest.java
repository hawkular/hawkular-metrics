package org.rhq.metrics.restServlet.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.rhq.metrics.core.RawNumericMetric;
import org.rhq.metrics.restServlet.influx.InfluxHandler;
import org.rhq.metrics.restServlet.influx.InfluxQuery;

import static org.junit.Assert.assertEquals;

/**
 * Some testing ..
 * @author Heiko W. Rupp
 */
public class MiscTest {

    @Test
    public void testQuantil() throws Exception {

        double[] input = {1,1,1,3,4,7,9,11,13,13};

        List<RawNumericMetric> metrics = new ArrayList<>(10);
        for (Double val: input) {
            RawNumericMetric m = new RawNumericMetric("x",val,0);
            metrics.add(m);
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
