package org.rhq.metrics.rest;

import com.google.common.base.Strings;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.path.xml.XmlPath;
import com.jayway.restassured.response.Response;

import org.junit.Assert;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.hasSize;


public class BaseTest extends AbstractTestBase {

    private static final String APPLICATION_JAVASCRIPT = "application/javascript";
    private static final long SIXTY_SECONDS = 60*1000L;



    @Test
    public void pingTest() throws Exception {
        Response jsonp = given()
                .expect()
                    .statusCode(200)
                .when()
                    .post("/ping")
                .then()
                    .contentType(ContentType.JSON)
                .extract()
                    .response();

        JsonPath jsonPath = new JsonPath(jsonp.asString());

        DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
        Date date = df.parse(jsonPath.getString("value"));
        Date now = new Date();

        long timeDifference = now.getTime() - date.getTime();

        // This is a bit problematic and depends on the system where the test is executing
        Assert.assertTrue("Difference is " + timeDifference, timeDifference < SIXTY_SECONDS);
    }

    @Test
    public void pingTestXml() throws Exception {
        Response response = given()
            .header(acceptXml)
            .expect()
                .statusCode(200)
                .contentType(ContentType.XML)
                .log().ifError()
            .when()
                .post("/ping")
            .then()
            .extract()
                .response();


        XmlPath xmlPath = new XmlPath(response.asString());
        String valueNode = xmlPath.get("value.@value");

        DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
        Date date = df.parse(valueNode);
        Date now = new Date();

        long timeDifference = now.getTime() - date.getTime();

        // This is a bit problematic and depends on the system where the test is executing
        Assert.assertTrue("Difference is " + timeDifference, timeDifference < SIXTY_SECONDS);
    }

    @Test
    public void pingTestWithJsonP() throws Exception {
        Response response =
            given()
                .header(acceptWrappedJson)
                .contentType(ContentType.JSON)
                .queryParam("jsonp", "jsonp") // Use jsonp-wrapping e.g. for JavaScript access
            .expect()
                .statusCode(200)
                .log().ifError()
            .when()
                .post("/ping")
            .then()
            .extract()
                .response();

        String mediaType = response.getContentType();
        assert mediaType != null : "Did not see a Content-Type header";
        assert mediaType.startsWith("application/javascript");

        // check for jsonp wrapping
        String bodyString = response.asString();
        assert bodyString != null;
        assert !bodyString.isEmpty();
        assert bodyString.startsWith("jsonp(");
        assert bodyString.endsWith(");");

        // extract the internal json data
        String body = bodyString.substring(6, bodyString.length() - 2);

        JsonPath jsonPath = new JsonPath(body);

        DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
        Date date = df.parse(jsonPath.getString("value"));
        Date now = new Date();

        long timeDifference = now.getTime() - date.getTime();

        Assert.assertTrue("Difference is " + timeDifference, timeDifference < SIXTY_SECONDS);
    }

    @Test
    public void testAddGetValue() throws Exception {

        String id = "foo";
        long now = System.currentTimeMillis();
        Map<String,Object> data = createDataPoint(id, now, 42d);

        postDataPoint(id, data);

        given()
            .pathParam("id", id)
            .header(acceptJson)
            .queryParam("start", now - 100)
            .queryParam("end", now + 100)
        .expect()
            .statusCode(200)
            .log().ifError()
            .body("timestamp[0]", equalTo(now))
        .when()
           .get("/metrics/{id}");
    }

    @Test
    public void testGetValueUnknownId() throws Exception {

        String id = "- fooDoesNotExist- ";
        long now = System.currentTimeMillis();

        given()
            .pathParam("id", id)
            .header(acceptJson)
            .queryParam("start", now - 100)
            .queryParam("end", now + 100)
        .expect()
            .statusCode(404)
            .log().status()
        .when()
           .get("/metrics/{id}");
    }

    @Test
    public void testListIds() throws Exception {

        String id = "fooListIds";
        long now = System.currentTimeMillis();
        Map<String,Object> data = createDataPoint(id, now, 42d);

        postDataPoint(id, data);

        String id2 = "fooListIds2";
        Map<String,Object> data2 = createDataPoint(id2, now, 42d);

        postDataPoint(id2, data2);

        given()
            .header(acceptJson)
        .expect()
            .statusCode(200)
            .body(not(emptyCollectionOf(String.class)))
        .log().everything()
        .when()
            .get("/metrics");


    }

    @Test
    public void testAddGetValueNoAcceptsHeaderForGet() throws Exception {

        String id = "fooNC";
        long now = System.currentTimeMillis();
        Map<String,Object> data = createDataPoint(id, now, 42d);

        postDataPoint(id, data);

        given()
            .pathParam("id", id)
            .queryParam("start", now - 100)
            .queryParam("end", now + 100)
        .expect()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .log().ifError()
            .body("timestamp[0]", equalTo(now))
        .when()
           .get("/metrics/{id}");
    }

    @Test
    public void testAddGetValueXml() throws Exception {

        String id = "fooXml1";
        long now = System.currentTimeMillis();
        Map<String,Object> data = createDataPoint(id, now, 42d);

        postDataPoint(id, data);

        XmlPath xmlPath =
        given()
            .pathParam("id", id)
            .header(acceptXml)
            .queryParam("start", now - 100)
            .queryParam("end", now + 100)
        .expect()
            .statusCode(200)
            .log().ifError()
        .when()
           .get("/metrics/{id}")
        .xmlPath();

        Long aLong = xmlPath.getLong("collection.dataPoint.timestamp[0]");

        assert now == aLong : "Timestamp was " +  aLong + " expected: " + now;
    }

//    @Test
    public void testAddGetValueWithJsonP() throws Exception {

        String id = "fooJsonP";
        long now = System.currentTimeMillis();
        Map<String,Object> data = createDataPoint(id, now, 42d);

        postDataPoint(id, data);

        Response response =
            given()
                .pathParam("id", id)
                .header(acceptWrappedJson)
                .queryParam("jsonp", "jsonp") // Use jsonp-wrapping e.g. for JavaScript access
                .queryParam("start", now - 100)
                .queryParam("end", now + 100)
            .expect()
                .statusCode(200)
                .log().all()
            .when()
               .get("/metrics/{id}");

        // We request our custom type, but the resulting document must have
        // a content type of application/javascript
        String mediaType = response.getContentType();
        assert mediaType != null : "Did not see a Content-Type header";
        assert mediaType.startsWith(APPLICATION_JAVASCRIPT) : "Media type was " + mediaType;

        // check for jsonp wrapping
        String bodyString = response.asString();
        System.out.println("Received body is [" + bodyString + "]");
        assert !Strings.isNullOrEmpty(bodyString) : "Body is empty";
        assert bodyString.startsWith("jsonp(") : "Body is not formatted as jsonp";
        assert bodyString.endsWith(");") : "Body is not formatted as jsonp";

        // extract the internal json data
        String body = bodyString.substring(6,bodyString.length()-2);
        assert !body.isEmpty() : "Inner body is empty and does not contain json data";

        JsonPath jp = new JsonPath(body);
        long aLong = jp.getLong("timestamp[0]");
        assert aLong == now : "Timestamp was " +  aLong + " expected: " + now;

    }

    @Test
    public void testAddGetValues() throws Exception {

        List<Map<String,Object>> data = new ArrayList<>();
        String id = "bla";
        long now = System.currentTimeMillis();

        Map<String,Object> data1 = createDataPoint(id, now - 10, 42d);
        Map<String,Object> data2 = createDataPoint(id, now, 4242d);
        data.add(data1);
        data.add(data2);

        postDataPoints(data);

        given()
            .pathParam("id", id)
          .header(acceptJson)
            .queryParam("start", now - 15)
            .queryParam("end", now + 5)
        .expect()
           .statusCode(200)
            .log().ifError()
            .body("", hasSize(2))
        .when()
           .get("/metrics/{id}");
    }

    @Test
    public void testAddGetBucketValues() throws Exception {

        List<Map<String,Object>> data = new ArrayList<>();
        String id = "bla";
        long now = System.currentTimeMillis();

        Map<String,Object> data1 = createDataPoint(id, now - 12, 42d);
        Map<String,Object> data2 = createDataPoint(id, now, 4242d);
        data.add(data1);
        data.add(data2);

        postDataPoints(data);

        given()
            .pathParam("id", id)
            .header(acceptJson)
            .queryParam("buckets", 3)
            .queryParam("start", now - 15)
            .queryParam("end", now + 15)
        .expect()
           .statusCode(200)
            .log().ifError()
            .body("", hasSize(3))
        .when()
           .get("/metrics/{id}");
    }

    @Test
    public void testAddGetBucketValues2() throws Exception {

        List<Map<String,Object>> data = new ArrayList<>();
        String id = "bla2";
        long now = System.currentTimeMillis();

        Map<String,Object> data1 = createDataPoint(id, now - 12, 42d);
        Map<String,Object> data2 = createDataPoint(id, now, 4242d);
        data.add(data1);
        data.add(data2);

        postDataPoints(data);

        given()
            .pathParam("id", id)
            .header(acceptJson)
            .queryParam("buckets", 3)
            .queryParam("bucketWidthSeconds",10)
            .queryParam("start",now-15)
            .queryParam("end",now+15)
        .expect()
           .statusCode(200)
            .log().ifError()
            .body("", hasSize(3))
        .when()
           .get("/metrics/{id}");
    }

    @Test
    public void testAddGetBucketValues3() throws Exception {

        List<Map<String,Object>> data = new ArrayList<>();
        String id = "bla3";
        long now = System.currentTimeMillis();

        Map<String,Object> data1;

        data1 = createDataPoint(id, now - 26000, 26d);
        data.add(data1);

        data1 = createDataPoint(id, now - 25000, 25d);
        data.add(data1);

        data1 = createDataPoint(id, now - 16000, 16d);
        data.add(data1);

        data1 = createDataPoint(id, now - 15000, 15d);
        data.add(data1);

        data1 = createDataPoint(id, now - 6000, 6d);
        data.add(data1);

        data1 = createDataPoint(id, now - 5000, 5d);
        data.add(data1);

        postDataPoints(data);

        List resultList =
        given()
            .pathParam("id", id)
            .header(acceptJson)
            .queryParam("buckets",3)
            .queryParam("bucketWidthSeconds",10)
            .queryParam("bucketCluster",false)
            .queryParam("start", now - 30000)
            .queryParam("end",now)
        .expect()
           .statusCode(200)
            .log().ifError()
            .body("", hasSize(6))
        .when()
           .get("/metrics/{id}")
        .as(List.class);

        assert resultList != null;
        assert resultList.size()==6;
        for (int i = 0; i < 6; i++) {
            @SuppressWarnings("unchecked")
            Map<String,Object> item = (Map<String, Object>) resultList.get(i);
            switch (i) {
            case 0:
                assert Integer.valueOf(String.valueOf(item.get("timestamp"))) == 0;
                assertDouble(item,26d,25d);
                break;
            case 1:
                assert Integer.valueOf(String.valueOf(item.get("timestamp"))) == 0;
                assertDouble(item,25d,26d);
                break;
            case 2:
                assert Integer.valueOf(String.valueOf(item.get("timestamp"))) == 10000;
                assertDouble(item,16d,15d);
                break;
            case 3:
                assert Integer.valueOf(String.valueOf(item.get("timestamp"))) == 10000;
                assertDouble(item,15d,16d);
                break;
            case 4:
                assert Integer.valueOf(String.valueOf(item.get("timestamp"))) == 20000;
                assertDouble(item,6d,5d);
                break;
            case 5:
                assert Integer.valueOf(String.valueOf(item.get("timestamp"))) == 20000;
                assertDouble(item,5d,6d);
                break;
            }
        }
    }

    @Test
    public void testDeleteMetric() throws Exception {

        String id = "fooDelete";
        long now = System.currentTimeMillis();
        Map<String,Object> data = createDataPoint(id, now, 42d);

        postDataPoint(id, data);

        given()
            .pathParam("id", id)
            .header(acceptJson)
            .queryParam("start", now - 100)
            .queryParam("end", now + 100)
        .expect()
            .statusCode(200)
            .log().ifError()
            .body("timestamp[0]", equalTo(now))
        .when()
           .get("/metrics/{id}");

        given()
            .pathParam("id", id)
            .header(acceptJson)
        .expect()
            .statusCode(200)
            .log().ifError()
        .when()
            .delete("/metrics/{id}");

        given()
            .pathParam("id", id)
            .header(acceptJson)
            .queryParam("start", now - 100)
            .queryParam("end", now + 100)
        .expect()
            .statusCode(404)
        .when()
           .get("/metrics/{id}");
    }





    private void assertDouble(Map<String, Object> item,double refVal, double refVal2) {
        Double value = Double.valueOf(String.valueOf(item.get("value")));
        boolean expr = value.compareTo(refVal) == 0 || value.compareTo(refVal2)==0;
        assert expr : "Value was " + value + " but should be " + refVal + " or " + refVal2;
    }

}
