package org.rhq.metrics.client.common;

import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SingleMetricTest {

    @Parameters(name = "toJson({0}, {1}, {2}) = {3}")
    public static Iterable<Object[]> testToJsonData() {
        return Arrays.asList(new Object[][]{
                {"a", 5, 3d, "{\"id\":\"a\",\"timestamp\":5,\"value\":3.0}"},
                {"b", 4, 2.15456d, "{\"id\":\"b\",\"timestamp\":4,\"value\":2.15456}"},
                {"c", 9, 7.2541d, "{\"id\":\"c\",\"timestamp\":9,\"value\":7.2541}"}
        });
    }

    private final String source;
    private final long timestamp;
    private final Double value;
    private final String expectedJson;

    public SingleMetricTest(String source, long timestamp, Double value, String expectedJson) {
        this.source = source;
        this.timestamp = timestamp;
        this.value = value;
        this.expectedJson = expectedJson;
    }

    @Test
    public void testToJson() throws Exception {
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
