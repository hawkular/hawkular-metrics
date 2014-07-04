package org.rhq.metrics.restServlet.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.rhq.metrics.core.RawNumericMetric;
import org.rhq.metrics.restServlet.InfluxHandler;

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
        Assert.assertEquals(2,q30,0.1);

        double q75 = new InfluxHandler().quantil(metrics,75);
        Assert.assertEquals(11,q75,0.1);

        double q50 = new InfluxHandler().quantil(metrics,50);
        Assert.assertEquals(5.5,q50,0.1);


    }
}
