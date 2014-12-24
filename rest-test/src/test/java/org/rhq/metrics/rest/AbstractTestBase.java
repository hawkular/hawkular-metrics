/*
 * Copyright 2014 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.rhq.metrics.rest;

import static com.jayway.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Header;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;

/**
 * Base class for tests
 * @author Heiko W. Rupp
 */
@RunWith(Arquillian.class)
public abstract class AbstractTestBase {

    static final String APPLICATION_JSON = "application/json";
    static final String WRAPPED_JSON = "application/vnd.rhq.wrapped+json";
    static final String APPLICATION_XML = "application/xml";
    static Header acceptJson = new Header("Accept", APPLICATION_JSON);
    static Header acceptWrappedJson = new Header("Accept", WRAPPED_JSON);
    static Header acceptXml = new Header("Accept", APPLICATION_XML);

    @Deployment(testable = false)
    public static WebArchive createDeployment() {
        String deploymentGroupId = System.getProperty("deployment.groupId");
        assertThat(deploymentGroupId).isNotNull().isNotEmpty();

        String deploymentArtifactId = System.getProperty("deployment.artifactId");
        assertThat(deploymentArtifactId).isNotNull().isNotEmpty();

        String deploymentPackaging = System.getProperty("deployment.packaging");
        assertThat(deploymentPackaging).isNotNull().isNotEmpty();

        String deploymentVersion = System.getProperty("deployment.version");
        assertThat(deploymentVersion).isNotNull().isNotEmpty();

        WebArchive[] webArchives = Maven //
                .configureResolver() //
                .workOffline() //
                .resolve( //
                        deploymentGroupId + ":" + deploymentArtifactId + ":" + deploymentPackaging + ":" + deploymentVersion //
                ) //
                .withoutTransitivity() //
                .as(WebArchive.class);
        assertThat(webArchives).isNotNull().isNotEmpty().hasSize(1);

        return webArchives[0];
    }

    @ArquillianResource
    private URL baseUrl;


    @Before
    public void setupRestAssured() {
        if (baseUrl!=null) {
            RestAssured.baseURI = baseUrl.toString();
        } else {
            RestAssured.baseURI = "http://localhost:8080/rhq-metrics";
        }
        // There is no need to set RestAssured.basePath as this is already in the baseUrl,
        // set via Arquillian in the baseUrl
    }

    @After
    public void tearDown() {
        RestAssured.reset();
    }

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
