package org.rhq.metrics.rest;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Header;


import static com.jayway.restassured.RestAssured.given;

/**
 * Base class for tests
 * @author Heiko W. Rupp
 */
public abstract class AbstractTestBase {

    static final String APPLICATION_JSON = "application/json";
    static final String WRAPPED_JSON = "application/vnd.rhq.wrapped+json";
    static final String APPLICATION_XML = "application/xml";
    static Header acceptJson = new Header("Accept", APPLICATION_JSON);
    static Header acceptWrappedJson = new Header("Accept", WRAPPED_JSON);
    static Header acceptXml = new Header("Accept", APPLICATION_XML);

    protected Map<String, Object> createDataPoint(String id, long time, Double value) {
        Map<String,Object> data = new HashMap<>(3);
        data.put("id", id);
        data.put("timestamp", time);
        data.put("value",value);

        return data;

    }

    protected void postDataPoint(String id, Map<String, Object> data) {
        given()
            .body(data)
            .pathParam("id", id)
            .contentType(ContentType.JSON)
            .expect()
            .statusCode(200)
            .when()
            .post("/metrics/{id}");
    }

    protected void postDataPoints(List<Map<String, Object>> data) {
        given()
            .body(data)
            .contentType(ContentType.JSON)
        .expect()
            .statusCode(200)
        .when()
            .post("/metrics");
    }
}
