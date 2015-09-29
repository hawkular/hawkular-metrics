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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.client.WebClient;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.util.HttpResponseCodes;
import org.junit.Assert;
import org.junit.Before;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.fabric8.arquillian.kubernetes.Session;
import io.fabric8.kubernetes.api.KubernetesClient;
import io.fabric8.kubernetes.api.model.KubernetesList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;

/**
 * @author mwringe
 */
public abstract class BaseContainerTests {

    protected static final String HAWKULAR_METRICS_NAME = "hawkular-metrics";
    protected static final String CASSANDRA_NAME = "hawkular-cassandra";
    protected static final String CASSANDRA_CLUSTER_NAME = "hawkular-cassandra-nodes";

    protected static final String CASSANDRA_MASTER_NAME = "hawkular-cassandra-master";

    protected static final String READY_STATE = "Ready";

    protected static String TENANT_HEADER = "Hawkular-Tenant";

    ResteasyWebTarget target;
    String metricsIP;


    @ArquillianResource
    protected static KubernetesClient client;

    @ArquillianResource
    protected static Session session;

    String getTenant() {
        return this.getClass().getSimpleName();
    }

    @Before
    public void setup() throws Exception {
        checkDeployment();
        Service service = client.getService(HAWKULAR_METRICS_NAME);
        metricsIP = service.getSpec().getClusterIP();

        assertTrue(metricsIP != null && !metricsIP.equalsIgnoreCase("None"));

        ResteasyClient restClient = new ResteasyClientBuilder().build();
        target = restClient.target("http://" + metricsIP);
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
        return getJSON(target, path, tenant);
    }

    public JsonNode getHTTPsJSON(String path, String tenant, byte[] certificate) throws Exception {

//        SSLContext sslContext = setupSSL(certificate);
//        ResteasyClient restClient = new ResteasyClientBuilder().sslContext(sslContext).build();

        ResteasyClient restClient = new ResteasyClientBuilder().disableTrustManager().build();
        ResteasyWebTarget target = restClient.target("https://" + metricsIP);

        return getJSON(target, path, tenant);
    }

    public JsonNode getJSON(ResteasyWebTarget target, String path, String tenant) throws Exception {
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


    public SSLContext setupSSL(byte[] certificateBytes) throws CertificateException, IOException, KeyStoreException,
            NoSuchAlgorithmException, KeyManagementException {

        CertificateFactory cFactory = CertificateFactory.getInstance("X.509");
        InputStream certInputStream = new ByteArrayInputStream(certificateBytes);

        // get the certificate
        Certificate certificate = cFactory.generateCertificate(certInputStream);
        certInputStream.close();

        // get the default keystore and add the certificate
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("ca", certificate);

        TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmFactory.init(keyStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmFactory.getTrustManagers(), null);

        return sslContext;
    }

    protected static void addClusterPolicy(String userName, String type) throws Exception {

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
        Response putResponse = webclient.path("/oapi/v1/clusterrolebindings/" + type)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .put(policy);

        assertEquals(200, putResponse.getStatus());
    }


    protected void checkDeployment() throws Exception {
        // if there is no 'hawkular-metrics' service available, then assume the template has not yet been deployed
        if (client.getService("hawkular-metrics") == null && client.getPodsForService("hawkular-metrics").isEmpty()) {
            System.out.println("ABOUT TO DEPLOY THE METRICS COMPONENTS");
            addClusterPolicy("metrics-deployer", "cluster-admins");

            // generate the service account and add its secret
            ServiceAccount serviceAccount = Util.createServiceAccount("metrics-deployer");
            serviceAccount.setSecrets(Util.createSimpleObjectReference("metrics-deployer"));
            // Create the secret in the Kubernetes cluster
            client.createServiceAccount(serviceAccount, session.getNamespace());

            Thread.sleep(1000);

            // Create the required secret in the Kubernetes cluster.
            // For this example its just an empty one, but still required for something to exist.
            client.createSecret(Util.createEmptySecret("metrics-deployer"), session.getNamespace());

            // Load the template
            Map<String, String> properties = new HashMap<>();
            properties.put("IMAGE_PREFIX", "hawkular/");
            properties.put("IMAGE_VERSION", "0.7.0-SNAPSHOT");
            properties.put("HAWKULAR_METRICS_HOSTNAME", "hawkular-metrics");

            URL url = this.getClass().getClassLoader().getResource("deployer.yaml");

            File file = new File(url.getFile());

            KubernetesList list = Util.processTemplate(file, properties);

            Util.deploy(client, list);

            Util.waitForService(client, "hawkular-cassandra");
            Util.waitForService(client, "hawkular-metrics");
            Util.waitForService(client, "heapster");

            System.out.println("EVERYTHING DEPLOYED. TESTS ARE READY TO BE RUN");
        }
    }
}
