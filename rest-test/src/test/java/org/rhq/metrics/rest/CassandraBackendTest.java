package org.rhq.metrics.rest;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.joda.time.DateTime.now;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.joda.time.DateTime;
import org.junit.Test;

/**
 * These are tests that exercise functionality only implemented in the Cassandra implementation.
 *
 * @author John Sanda
 */
public class CassandraBackendTest extends AbstractTestBase {

    @Test
    public void insertNumericDataForMultipleMetrics() {
        DateTime start = now().minusMinutes(10);

        given()
            .body(ImmutableMap.of("id", "tenant-1"))
            .contentType(JSON)
            .expect().statusCode(200).when().post("/tenants");

        List<Metric> requestBody = asList(
            new Metric("m1", asList(new DataPoint(start, 1.1), new DataPoint(start.plusMinutes(1), 1.2))),
            new Metric("m2", asList(new DataPoint(start, 2.1), new DataPoint(start.plusMinutes(1), 2.2))),
            new Metric("m3", asList(new DataPoint(start, 3.1), new DataPoint(start.plusMinutes(1), 3.2)))
        );

        given().body(requestBody).pathParam("tenantId", "tenant-1").contentType(JSON)
        .expect().statusCode(200)
        .when().post("/{tenantId}/metrics/numeric/data");

        given().pathParam("tenantId", "tenant-1").pathParam("id", "m2").contentType(JSON)
        .expect().statusCode(200)
        .when().get("/{tenantId}/metrics/numeric/{id}/data")
        .then().assertThat().body("data", equalTo(asList(data(start.plusMinutes(1), 2.2), data(start, 2.1))));
    }

    @Test
    public void insertAvailabilityDataForMultipleMetrics() {
        DateTime start = now().minusMinutes(10);
        List<Metric> requestBody = asList(
            new Metric("m1", asList(new DataPoint(start, "down"), new DataPoint(start.plusMinutes(1), "up"))),
            new Metric("m2", asList(new DataPoint(start, "up"), new DataPoint(start.plusMinutes(1), "up"))),
            new Metric("m3", asList(new DataPoint(start, "down"), new DataPoint(start.plusMinutes(1), "down")))
        );

        given().body(ImmutableMap.of("id", "tenant-2")).contentType(JSON)
        .expect().statusCode(200)
        .when().post("/tenants");

        given().body(requestBody).pathParam("tenantId", "tenant-2").contentType(JSON)
        .expect().statusCode(200)
        .when().post("/{tenantId}/metrics/availability/data");

        given().pathParam("tenantId", "tenant-2").pathParam("id", "m1").contentType(JSON)
        .expect().statusCode(200)
        .when().get("/{tenantId}/metrics/availability/{id}/data")
        .then().assertThat().body("data", equalTo(asList(data(start.plusMinutes(1), "up"), data(start, "down"))));
    }

    @Test
    public void createMetricsAndUpdateMetadata() {
        Map<String, ? extends Object> numericData = ImmutableMap.of(
            "name", "N1",
            "metadata", ImmutableMap.of("a1", "A", "B1", "B")
        );

        // Create a numeric metric
        given()
            .body(numericData).pathParam("tenantId", "tenant-3").contentType(JSON)
            .expect().statusCode(200)
            .when().post("/{tenantId}/metrics/numeric");

        // Make sure we do not allow duplicates
        given()
            .body(numericData).pathParam("tenantId", "tenant-3").contentType(JSON)
            .expect().statusCode(400)
            .when().post("/{tenantId}/metrics/numeric")
            .then().assertThat().body("errorMsg", not(isEmptyOrNullString()));

        Map<String, ? extends Object> availabilityData = ImmutableMap.of(
            "name", "A1",
            "metadata", ImmutableMap.of("a2", "2", "b2", "2")
        );

        // Create an availability metric
        given()
            .body(availabilityData).pathParam("tenantId", "tenant-3").contentType(JSON)
            .expect().statusCode(200)
            .when().post("/{tenantId}/metrics/availability");

        // Make sure we do not allow duplicates
        given()
            .body(availabilityData).pathParam("tenantId", "tenant-3").contentType(JSON)
            .expect().statusCode(400)
            .when().post("/{tenantId}/metrics/availability")
            .then().assertThat().body("errorMsg", not(isEmptyOrNullString()));

        // Fetch numeric meta data
        given()
            .pathParam("tenantId", "tenant-3").pathParam("id", "N1").contentType(JSON)
            .expect().statusCode(200)
            .when().get("/{tenantId}/metrics/numeric/{id}/meta")
            .then().assertThat().body("tenantId", equalTo("tenant-3")).and().body("name", equalTo("N1")).and()
                .body("metadata", equalTo(ImmutableMap.of("a1", "A", "B1", "B")));

        // Verify the response for a non-existent metric
        given()
            .pathParam("tenantId", "tenant-3").pathParam("id", "N2").contentType(JSON)
            .expect().statusCode(204)
            .when().get("/{tenantId}/metrics/numeric/{id}/meta");

        // Fetch availability metric meta data
        given()
            .pathParam("tenantId", "tenant-3").pathParam("id", "A1").contentType(JSON)
            .expect().statusCode(200)
            .when().get("/{tenantId}/metrics/availability/{id}/meta")
            .then().assertThat().body("tenantId", equalTo("tenant-3")).and().body("name", equalTo("A1")).and()
            .body("metadata", equalTo(ImmutableMap.of("a2", "2", "b2", "2")));

        // Verify the response for a non-existent metric
        given()
            .pathParam("tenantId", "tenant-3").pathParam("id", "A2").contentType(JSON)
            .expect().statusCode(204)
            .when().get("/{tenantId}/metrics/availability/{id}/meta");
    }

    private Map<String, ? extends Object> data(DateTime timestamp, double value) {
        Map<String, Object> map = new HashMap<>();
        map.put("timestamp", timestamp.getMillis());
        map.put("value", new Float(value));
        // Data points include a tags property even when they have none. We need to
        // configure Jackson to suppress null fields.
        map.put("tags", null);

        return map;
    }

    private Map<String, ? extends Object> data(DateTime timestamp, String value) {
        Map<String, Object> map = new HashMap<>();
        map.put("timestamp", timestamp.getMillis());
        map.put("value", value);
        // Data points include a tags property even when they have none. We need to
        // configure Jackson to suppress null fields.
        map.put("tags", null);

        return map;
    }

    private static class Metric {
        private String name;
        private List<DataPoint> data;

        public Metric(String name, List<DataPoint> data) {
            this.name = name;
            this.data = data;
        }

        public String getName() {
            return name;
        }

        public List<DataPoint> getData() {
            return data;
        }
    }

    private static class DataPoint {
        private long timestamp;
        private Object value;

        public DataPoint() {
        }

        public DataPoint(long timestamp, Object value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        public DataPoint(DateTime timestamp, Object value) {
            this.timestamp = timestamp.getMillis();
            this.value = value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }

}
