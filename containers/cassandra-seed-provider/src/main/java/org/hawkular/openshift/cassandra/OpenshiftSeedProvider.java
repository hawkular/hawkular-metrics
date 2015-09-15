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

package org.hawkular.openshift.cassandra;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SeedProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mwringe on 10/04/15.
 */
public class OpenshiftSeedProvider implements SeedProvider {

    private static final Logger logger = LoggerFactory.getLogger(OpenshiftSeedProvider.class);

    private static final String PARAMETER_SEEDS = "seeds";

    // The environment variables to set the Cassandra Service Name
    private static final String CASSANDRA_NODES_SERVICE_NAME =
            getEnv("CASSANDRA_NODES_SERVICE_NAME", "hawkular-cassandra");


    // The environment variables to set the KUBERNETES-MASTER
    private static final String KUBERNETES_MASTER_URL =
            getEnv("KUBERNETES_MASTER_URL", "https://kubernetes.default.svc.cluster.local:443");

    // The environment vairables to set the namespace
    private static final String POD_NAMESPACE = getEnv("POD_NAMESPACE", "default");

    private static final String KUBERNETES_MASTER_CERTIFICATE_FILENAME = "/var/run/secrets/kubernetes" +
            ".io/serviceaccount/ca.crt";

//    private static final String SERVICE_ACCOUNT_TOKEN_FILENAME =
//                                                      "/var/run/secrets/kubernetes.io/serviceaccount/token";
//
//    private static final String SERVICE_URL = KUBERNETES_MASTER_URL + "/api/v1/namespaces/" + POD_NAMESPACE +
//            "/endpoints/" + CASSANDRA_NODES_SERVICE_NAME;

    private static final String MASTER = getEnv("CASSANDRA_MASTER", "false");



    // properties to determine how long to wait for the service to come online.
    // Currently set to 60 seconds
    private static final int SERVICE_TRIES = 30;
    private static final int SERVICE_TRY_WAIT_TIME_MILLISECONDS = 2000;

    private SSLContext sslContext;

    //Required Constructor
    public OpenshiftSeedProvider(Map<String, String> args) {
        try {
            sslContext = setupSSL();
        } catch (Exception e) {
            throw new RuntimeException("Could not setup security properly for Cassandra", e);
        }
    }

    @Override
    public List<InetAddress> getSeeds() {
        List<InetAddress> seeds = new ArrayList<>();

        try {
            InetAddress[] inetAddresses = null;

            for (int i = 0; i < SERVICE_TRIES; i++) {
                List<InetAddress> serviceList = getInetAddresses(CASSANDRA_NODES_SERVICE_NAME);
                if (!serviceList.isEmpty()) {
                    seeds = serviceList;
                    break;
                }

                // If we are a master node and we didn't find another other nodes in the service, then we assume
                // We are the only node available, so we will use our own IP address listed in the seed configuration
                // file.
                if (MASTER.equalsIgnoreCase("true") && i >= 3) {
                    logger.debug("Did not find any other nodes running in the cluster and this instance is " +
                                         "marked as a Master. Configuring this node to use its own IP as a seed.");
                    return getSeedsFromConfig();
                }

                Thread.sleep(SERVICE_TRY_WAIT_TIME_MILLISECONDS);
            }
        }
        catch(Exception exception) {
            logger.error("Could not resolve the list of seeds for the Cassandra cluster. Aborting.", exception);
            throw new RuntimeException(exception);
        }

        return seeds;
    }

    private SSLContext setupSSL() throws CertificateException, IOException, KeyStoreException,
            NoSuchAlgorithmException, KeyManagementException {
        CertificateFactory cFactory = CertificateFactory.getInstance("X.509");
        InputStream certInputStream = new FileInputStream(KUBERNETES_MASTER_CERTIFICATE_FILENAME);

        // get the certificate
        Certificate certificate = cFactory.generateCertificate(certInputStream);
        certInputStream.close();

        // get the default keystore and add the certificate
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("kubernetes_master_ca", certificate);

        TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmFactory.init(keyStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmFactory.getTrustManagers(), null);

        return sslContext;
    }

    private List<InetAddress> getInetAddresses(String serviceName)
            throws UnknownHostException, InterruptedException {

        List<InetAddress> inetAddresses = new ArrayList<>();

        try {
            InetAddress[] inetArray = InetAddress.getAllByName(serviceName);

            for (InetAddress address: inetArray) {
                inetAddresses.add(address);
            }

        } catch (UnknownHostException e) {
            logger.warn("UnknownHostException for service '" + serviceName + "'. It may not be up yet. Trying again");
        }
        return inetAddresses;
    }

// Keeping this in here for now as we might need to go back to use the API to do a service lookup
//
//    private List<InetAddress> getInetAddresses(String serviceName) throws IOException {
//        URL url = new URL(SERVICE_URL);
//        HttpsURLConnection httpsURLConnection = (HttpsURLConnection) url.openConnection();
//
//        BufferedReader bufferedReader = new BufferedReader(new FileReader(SERVICE_ACCOUNT_TOKEN_FILENAME));
//        httpsURLConnection.setRequestProperty("Authorization", "Bearer " + bufferedReader.readLine());
//        bufferedReader.close();
//
//        httpsURLConnection.setSSLSocketFactory(sslContext.getSocketFactory());
//
//        ObjectMapper objectMapper = new ObjectMapper();
//
//        ObjectNode objectNode = (ObjectNode) objectMapper.readTree(httpsURLConnection.getInputStream());
//
//        return getInetAddresses(objectNode);
//    }
//
//    private List<InetAddress> getInetAddresses(ObjectNode objectNode) throws UnknownHostException {
//        List<InetAddress> seeds = new ArrayList<>();
//
//        ArrayNode subsets = (ArrayNode) objectNode.get("subsets");
//
//        if (subsets != null) {
//            for (JsonNode jsonNode : subsets) {
//                ArrayNode addresses = (ArrayNode) jsonNode.get("addresses");
//                if (addresses != null) {
//                    for (JsonNode address: addresses) {
//                        String ip = address.get("ip").asText();
//
//                        InetAddress inetAddress = InetAddress.getByName(ip);
//                        seeds.add(inetAddress);
//                    }
//                }
//            }
//
//        }
//        return seeds;
//    }

    private List<InetAddress> getSeedsFromConfig() throws ConfigurationException {
        List<InetAddress> seedsAddresses = new ArrayList<>();

        Config config = DatabaseDescriptor.loadConfig();
        String seeds = config.seed_provider.parameters.get(PARAMETER_SEEDS);

        for (String seed: seeds.split(",")) {
            try {
                InetAddress inetAddress = InetAddress.getByName(seed);
                logger.debug("Adding seed '" + inetAddress.getHostAddress() + "' from the configuration file.");
                seedsAddresses.add(inetAddress);
            } catch (UnknownHostException e) {
                logger.warn("Could not get address for seed entry '" + seed + "'", e);
            }
        }

        return seedsAddresses;
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value != null) {
            return value;
        } else {
            return defaultValue;
        }
    }
}
