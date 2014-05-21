package org.rhq.metrics.rest;

import static com.jayway.restassured.RestAssured.given;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.response.Response;


public class BaseTest {

    @Test
    public void pingTest() throws Exception {
        Response jsonp = given()
                .expect()
                    .statusCode(200)
                .when()
                    .post("/rhq-metrics/ping")
                .then()
                    .contentType(ContentType.JSON)
                .extract()
                    .response();

        JsonPath jsonPath = new JsonPath(jsonp.asString());

        DateFormat df = new SimpleDateFormat("EEE MMM d H:mm:ss z yyyy");
        Date date = df.parse(jsonPath.getString("pong"));
        Date now = new Date();

        long timeDifference = now.getTime() - date.getTime();

        Assert.assertTrue(timeDifference < 1000L);
    }
}