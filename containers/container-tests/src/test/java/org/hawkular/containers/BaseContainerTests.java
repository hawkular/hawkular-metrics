/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
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
package org.hawkular.containers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.arquillian.kubernetes.Session;
import io.fabric8.kubernetes.api.KubernetesClient;
import io.fabric8.kubernetes.api.model.Service;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.util.HttpResponseCodes;
import org.junit.Assert;
import org.junit.Before;

/**
 * @author mwringe
 */
public abstract class BaseContainerTests {

    protected static final String HAWKULAR_METRICS_NAME = "hawkular-metrics";
    protected static final String CASSANDRA_NAME = "hawkular-cassandra";
    protected static final String CASSANDRA_CLUSTER_NAME = "hawkular-cassandra-nodes";

    protected static final String READY_STATE = "Ready";

    protected static String TENANT_HEADER = "Hawkular-Tenant";

    ResteasyWebTarget target;
    String metricsIP;


    @ArquillianResource
    protected KubernetesClient client;

    @ArquillianResource
    protected Session session;

    String getTenant() {
        return this.getClass().getSimpleName();
    }

    @Before
    public void setup() throws Exception {
        Service service = client.getService(HAWKULAR_METRICS_NAME);
        metricsIP = service.getSpec().getPortalIP();
        assertTrue(metricsIP != null && !metricsIP.equalsIgnoreCase("None"));

        ResteasyClient restClient = new ResteasyClientBuilder().build();
        target = restClient.target("http://" + metricsIP);
    }



    protected void foo() throws Exception {
        ResteasyClient restClient = new ResteasyClientBuilder().build();
        ResteasyWebTarget target = restClient.target("http://localhost:8080");

    }

    public void sendMetric(String metricId, String metricValue, int timeStamp) throws Exception {

        String body = "[{\"value\": " + metricValue + ",\"timestamp\":" + timeStamp + "}]";

        Response response = target.clone()
                .path("/hawkular/metrics/gauges/" + metricId + "/data")
                .queryParam("start", 0)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .header(TENANT_HEADER, getTenant())
                .post(Entity.entity(body, MediaType.APPLICATION_JSON_TYPE));

        //Note: its a 200 response and not a 201, since its posting data and not 'creating' the resource
        assertEquals(HttpResponseCodes.SC_OK, response.getStatus());
        response.close();
    }

    public JsonNode getJSON(String path) throws Exception{
        return getJSON(path, getTenant());
    }

    public JsonNode getJSON(String path, String tenant) throws Exception {
        Response response = target.clone()
                .path(path)
                .queryParam("start", 0)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .header(TENANT_HEADER, tenant)
                .get();

        if (response.getStatus() == HttpResponseCodes.SC_NO_CONTENT) {
            assertNull(response.readEntity(String.class));
            return null;
        }

        Assert.assertEquals(HttpResponseCodes.SC_OK, response.getStatus());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(response.readEntity(String.class));
        response.close();

        return jsonNode;
    }

}
