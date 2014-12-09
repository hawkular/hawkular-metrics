package org.rhq.metrics.rest;

import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * Test the Influx Handler
 * @author Heiko W. Rupp
 */
public class InfluxTest extends AbstractTestBase {

    @Test
    public void testInfluxDataOrderedAscByDefault() {
        String id = "influx.dataasc";
        long now = System.currentTimeMillis();

        List<Map<String,Object>> data = new ArrayList<>();

        data.add(createDataPoint(id, now - 2000, 40d));
        data.add(createDataPoint(id, now - 1000, 41d));
        data.add(createDataPoint(id, now, 42d));

        postDataPoints(data);


        String query = "select value from  \"" + id + "\" order asc";

        given()
            .queryParam("q", query)
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
                    .body("[0].name", is(id))
                .and()
                    .body("[0].points[0][1]", hasToString("40.0"))
                .and()
                    .body("[0].points[1][1]", hasToString("41.0"))
                .and()
                    .body("[0].points[2][1]", hasToString("42.0"));
    }

    @Test
    public void testInfluxDataOrderedDescByDefault() {
        String id = "influx.datadesc";
        long now = System.currentTimeMillis();

        List<Map<String,Object>> data = new ArrayList<>();

        data.add(createDataPoint(id, now - 2000, 40d));
        data.add(createDataPoint(id, now - 1000, 41d));
        data.add(createDataPoint(id, now, 42d));

        postDataPoints(data);


        String query = "select value from  \"" + id + "\"";

        given()
            .queryParam("q", query)
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
                    .body("[0].name", is(id))
                .and()
                    .body("[0].points[0][1]", hasToString("42.0"))
                .and()
                    .body("[0].points[1][1]", hasToString("41.0"))
                .and()
                    .body("[0].points[2][1]", hasToString("40.0"));
    }

    @Test
    public void testInfluxAddGetOneMetric() throws Exception {

        String id = "influx.foo";
        long now = System.currentTimeMillis();

        List<Map<String,Object>> data = new ArrayList<>();

        data.add(createDataPoint(id, now - 2000, 40d));
        data.add(createDataPoint(id, now - 1000, 41d));
        data.add(createDataPoint(id, now, 42d));

        postDataPoints(data);


        String query = "select mean(value) from  \"influx.foo\" where time > now() - 30s group by time(30s) ";


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
                .body("[0].name", is("influx.foo"))
            .and()
                .body("[0].points[0][1]", hasToString("41.0"));

    }


    @Test
    public void testInfluxAddGetOneSillyMetric() throws Exception {

        String id = "influx.foo3";
        long now = System.currentTimeMillis();

        List<Map<String,Object>> data = new ArrayList<>();

        data.add(createDataPoint(id, now - 2000, 40d));
        data.add(createDataPoint(id, now - 1000, 41d));
        data.add(createDataPoint(id, now, 42d));

        postDataPoints(data);


        String query = "select mean(value) from  \"influx.foo3\" where time > '2013-08-12 23:32:01.232' and time < '2013-08-13' group by time(30s) ";


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
                .body("[0].name", is("influx.foo3"))
            .and()
                .body("[0].points[0]", Matchers.nullValue());

    }

    @Test
    public void testInfluxAddGetMultiMetric() throws Exception {

        String id = "influx.foo2";
        long now = System.currentTimeMillis();

        List<Map<String,Object>> data = new ArrayList<>();

        data.add(createDataPoint(id, now-2000, 40d));
        data.add(createDataPoint(id, now-1000, 41d));
        data.add(createDataPoint(id, now, 42d));

        id = "influx.bar2";
        data.add(createDataPoint(id, now-2000, 20d));
        data.add(createDataPoint(id, now-1000, 21d));
        data.add(createDataPoint(id, now, 22d));

        postDataPoints(data);

        String query = "select mean(value) from  \"influx.bar2\" where time > now() - 30s group by time(30s) ";


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
                .body("[0].name", is("influx.bar2"))
            .and()
                .body("[0].points[0][1]", hasToString("21.0"));

        query = "select mean(value) from  \"influx.foo2\" where time > now() - 30s group by time(30s) ";

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
                .body("[0].name", is("influx.foo2"))
            .and()
                .body("[0].points[0][1]", hasToString("41.0"));

    }

}
