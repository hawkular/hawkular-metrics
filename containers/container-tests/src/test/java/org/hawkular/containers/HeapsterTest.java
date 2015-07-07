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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import java.net.URLEncoder;
import java.util.List;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.cxf.jaxrs.client.WebClient;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author mwringe
 */
@RunWith(Arquillian.class)
public class HeapsterTest extends BaseContainerTests {

    private static final String HEAPSTER_SERVICE_ACCOUNT_NAME = "heapster";
    private static final String HEAPSTER_SERVICE_NAME = "heapster";

    @Test
    public void checkHeapsterAccount() throws Exception {
        // check that we have a heapster service account created
        List<ServiceAccount> serviceAccounts = client.getServiceAccounts(client.getNamespace()).getItems();
        boolean found = false;
        for (ServiceAccount serviceAccount: serviceAccounts) {
            if (serviceAccount.getMetadata().getName().equals(HEAPSTER_SERVICE_ACCOUNT_NAME)) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    @Test
    public void testHeapster() throws Exception {
        // add the policy so that the heapster user can access the metrics
        addClusterPolicy(HEAPSTER_SERVICE_ACCOUNT_NAME, "cluster-readers");

        // we need to sleep for a bit for heapster to gather some metrics
        Thread.sleep(10000);

        Service service = client.getService(HEAPSTER_SERVICE_NAME);
        String heapsterAddress = service.getSpec().getPortalIP();


        ResteasyClient restClient = new ResteasyClientBuilder().build();
        ResteasyWebTarget target = restClient.target("http://" + heapsterAddress);
        Response response = target.clone()
                .path("/validate")
                .request(MediaType.TEXT_PLAIN_TYPE)
                .get();

        assertEquals(200, response.getStatus());

        String responseString = response.readEntity(String.class);

        // check that the result returned by heapster includes some metrics:
        String metrics_phrase = "Known metrics: ";
        int subInt = responseString.lastIndexOf(metrics_phrase) + metrics_phrase.length();
        String metrics = responseString.substring(subInt);

        assertTrue(Integer.parseInt(metrics) > 0);
    }

    @Test
    public void testGetMetrics() throws Exception {
        String metricId = URLEncoder.encode("///cpu/usage", "UTF-8");

        Thread.sleep(10000);

        JsonNode json = getJSON("/hawkular/metrics/gauges/" + metricId + "/data", "heapster");

        assertNotNull(json);
        assertTrue(json instanceof ArrayNode);
        ArrayNode jsonArray = (ArrayNode)json;
        assertTrue(json.size() > 0);

        ObjectNode metric = (ObjectNode)jsonArray.get(0);
        long timeStamp = metric.get("timestamp").asLong();
        double value = metric.get("value").asDouble();

        assertTrue(timeStamp > 0);
        assertTrue(value > 0);

        metricId = URLEncoder.encode("hawkular-metrics/" + client.getService("hawkular-metrics", session.getNamespace
                ()).getMetadata().getName() + "/memory/usage", "UTF-8");

        List<Pod> pods4Service = client.getPodsForService(client.getService("hawkular-metrics", session.getNamespace
                ()));
        for (Pod pod: pods4Service) {
            metricId = URLEncoder.encode("hawkular-metrics/" + pod.getMetadata().getUid() + "/memory/usage", "UTF-8");

            json = getJSON("/hawkular/metrics/gauges/" + metricId + "/data", "heapster");

            metric = (ObjectNode)((ArrayNode)json).get(0);
            timeStamp = metric.get("timestamp").asLong();
            value = metric.get("value").asDouble();

            assertTrue(timeStamp > 0);
            assertTrue(value > 0);
        }
    }


    private void addClusterPolicy(String userName, String type) throws Exception {

        String path = "/oapi/v1/clusterrolebindings/" + type;
        String serviceAccount = "system:serviceaccount:" + session.getNamespace() + ":" + userName;

        // update the existing policy
        WebClient webclient = client.getFactory().createWebClient();
        ObjectNode policy = webclient.path(path)
                .accept(MediaType.APPLICATION_JSON)
                .get(ObjectNode.class);
        webclient.close();

        ArrayNode userNames = (ArrayNode) policy.get("userNames");
        userNames.add(serviceAccount);

        webclient = client.getFactory().createWebClient();
        Response putResponse = webclient.path("/oapi/v1/clusterrolebindings/cluster-readers")
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .put(policy);

        assertEquals(200, putResponse.getStatus());

    }
}
