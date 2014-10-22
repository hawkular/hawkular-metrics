package org.rhq.metrics.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.given;

/**
 * Test the Influx Handler
 * @author Heiko W. Rupp
 */
public class InfluxTest extends AbstractTestBase {

    @Test
    public void testInfluxAddGetOneMetric() throws Exception {

        String id = "influx.foo";
        long now = System.currentTimeMillis();

        List<Map<String,Object>> data = new ArrayList<>();

        data.add(createDataPoint(id, now - 2000, 40d));
        data.add(createDataPoint(id, now - 1000, 41d));
        data.add(createDataPoint(id, now, 42d));

        postDataPoints(data);


        String query = "select mean(value) from  \"influx.foo\" where time > now() - 30sec group by time(30s) order asc ";


        given()
            .queryParam("q",query)
            .header(acceptJson)
        .expect()
            .statusCode(200)
            .log()
                .ifError()
        .when()
            .get("/influx/series")
        .andReturn()
            .then()
            .assertThat()
                .body("[0].name", Matchers.is("influx.foo"))
            .and()
                .body("[0].points[0][1]", Matchers.hasToString("41.0"));

    }

}
