package org.rhq.metrics.client.common;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SingleMetricTest {

    @DataProvider(name = "testToJson")
    public Object[][] testToJsonData() {
        return new Object[][] {
            { "a", 5, 3d, "{\"id\":\"a\",\"timestamp\":5,\"value\":3.0}" },
            { "b", 4, 2.15456d, "{\"id\":\"b\",\"timestamp\":4,\"value\":2.15456}" },
            { "c", 9, 7.2541d, "{\"id\":\"c\",\"timestamp\":9,\"value\":7.2541}" }
        };
    }

    @Test(dataProvider = "testToJson")
    public void testToJson(String source, long timestamp, Double value, String expectedJson) throws Exception {
        SingleMetric metric = new SingleMetric(source, timestamp, value);
        assertEquals(metric.toJson(), expectedJson);
    }

    @Test
    public void testEquals() throws Exception {

        SingleMetric metric1 = new SingleMetric("bla",1,45d);
        SingleMetric metric2 = new SingleMetric("bla",1,99d);

        assert metric1.equals(metric2);

    }

    @Test
    public void testNotEquals() throws Exception {

        SingleMetric metric1 = new SingleMetric("foo",1,42d);
        SingleMetric metric2 = new SingleMetric("bla",1,42d);

        assert !metric1.equals(metric2);

    }
}
