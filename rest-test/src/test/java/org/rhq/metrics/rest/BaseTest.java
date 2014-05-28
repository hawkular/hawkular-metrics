package org.rhq.metrics.rest;

import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import java.io.File;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.archive.importer.MavenImporter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.response.Response;

@RunWith(Arquillian.class)
public class BaseTest {

    @Deployment(testable=false)
    public static WebArchive createDeployment() {
        File pomFile = new File("../rest-servlet/pom.xml");

        System.out.println("Pom file path: " + pomFile.getAbsolutePath());
        System.out.flush();

        WebArchive archive =
        ShrinkWrap.create(MavenImporter.class)
            .offline()
            .loadPomFromFile(pomFile)
            .importBuildOutput()
            .as(WebArchive.class);
        System.out.println("archive is " + archive.toString(false));
        System.out.flush();
        return archive;
    }

    @ArquillianResource
    private URL baseUrl;

    @Before
    public void setupRestAssured() {
        RestAssured.baseURI = baseUrl.toString();
        // There is no need to set RestAssured.basePath as this is already in the baseUrl,
        // set via Arquillian in the baseUrl
    }


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
        Date date = df.parse(jsonPath.getString("pong"));
        Date now = new Date();

        long timeDifference = now.getTime() - date.getTime();

        Assert.assertTrue("Difference is " + timeDifference, timeDifference < 2500L);
    }

    @Test
    public void testAddGetValue() throws Exception {

        String id = "foo";
        long now = System.currentTimeMillis();
        Map<String,Object> data = createDataPoint(id, now, 42d);

        given()
            .body(data)
            .pathParam("id",id)
            .contentType(ContentType.JSON)
        .expect()
            .statusCode(200)
        .when()
            .post("/metrics/{id}");


        given()
            .pathParam("id", id)
            .header("Accepts", "application/json")
        .expect()
            .statusCode(200)
            .log().ifError()
            .body("timestamp[0]", equalTo(now))
        .when()
           .get(new URL(baseUrl, "/rhq-metrics/metrics/{id}"));
    }

    @Test
    public void testAddGetValues() throws Exception {

        List<Map<String,Object>> data = new ArrayList<>();
        String id = "bla";
        long now = System.currentTimeMillis();

        Map<String,Object> data1 = createDataPoint(id,now-10,42d);
        Map<String,Object> data2 = createDataPoint(id,now,4242d);
        data.add(data1);
        data.add(data2);

        given()
            .body(data)
            .contentType(ContentType.JSON)
        .expect()
            .statusCode(200)
        .when()
            .post("/metrics");


        given()
            .pathParam("id", id)
          .header("Accepts", "application/json")
            .queryParam("start",now-15)
            .queryParam("end",now+5)
        .expect()
           .statusCode(200)
            .log().ifError()
            .body("", hasSize(2))
        .when()
           .get("/metrics/{id}");
    }

    private Map<String, Object> createDataPoint(String id, long time, Double value) {
        Map<String,Object> data = new HashMap<>(3);
        data.put("id", id);
        data.put("timestamp", time);
        data.put("value",value);

        return data;

    }
}
