/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.openshift.auth.org.hawkular.openshift.namespace;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.HttpsURLConnection;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 */
public class NamespaceListener {

    private static final Logger log = Logger.getLogger(NamespaceListener.class);

    // The authentication token used by the filter to connect to the OpenShift Master API
    private final String token;

    // Maps the namespace name to the namespace id
    private final Map<String, String> namespaces;

    private boolean run = true;

    /**
     *
     * @param token The token to be used by the
     */
    public NamespaceListener(String token) {
        log.debug("Creating NamespaceListener");
        this.token = token;

        namespaces = new ConcurrentHashMap<>();
        log.debug("Namespace listener starting.");
        new Thread(new Listener()).start();
    }

    public void stop() {
        log.debug("Namespace listener shutting down.");
        run = false;
    }

    /**
     * Returns the namespace id for a particular namespace name
     * @param namespaceName The name of the namespace
     * @return The id of the namespace or null if the namespace does not exist
     */
    public String getNamespaceId(String namespaceName) {
        return namespaces.computeIfAbsent(namespaceName, n -> getProjectId(namespaceName, token));
    }

    private static String getProjectId(String projectName, String token) {
        String projectId = null;

        HttpsURLConnection connection = null;
        InputStream inputStream = null;
        JsonParser jsonParser = null;

        try {
            String path = "api/v1/namespaces/" + projectName;
            URL url = new URL(NamespaceHandler.KUBERNETES_MASTER_URL + "/" + path);

            connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization", "Bearer " + token);

            connection.connect();
            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {

                JsonFactory jsonFactory = new JsonFactory();
                jsonFactory.setCodec(new ObjectMapper());

                inputStream = connection.getInputStream();
                jsonParser = jsonFactory.createParser(inputStream);
                JsonNode jsonNode = jsonParser.readValueAsTree();
                if (jsonNode != null) {
                    JsonNode metadata = jsonNode.get("metadata");
                    projectId = metadata.get("uid").asText();
                }

            } else {
                log.warnf("Error getting project metadata. Code %s: %s", responseCode, connection.getResponseMessage());
            }

        } catch (IOException io) {
            log.error(io);
        } finally {
            try {
                if (jsonParser != null) {
                    jsonParser.close();
                }
            }
            catch (IOException io) {
                log.error(io);
            }

            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException io) {
                log.error(io);
            }

            if (connection != null) {
                connection.disconnect();
            }
        }

        return projectId;
    }

    private class Listener implements Runnable {

        @Override
        public void run() {
            while(run && !Thread.currentThread().isInterrupted()) {
                try {
                    String path = "api/v1/namespaces";
                    URL url = new URL(NamespaceHandler.KUBERNETES_MASTER_URL + "/" + path + "?watch=true");

                    HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
                    connection.setRequestMethod("GET");

                    connection.setRequestProperty("Accept", "application/json");
                    connection.setRequestProperty("Authorization", "Bearer " + token);

                    connection.setConnectTimeout(5000);

                    // Since the connection is left open for new events, set to 0 which indicates an infinite timeout
                    // Note that the server will eventually close this connection, so we will need to reconnect when this happens
                    connection.setReadTimeout(0);

                    connection.connect();
                    int responseCode = connection.getResponseCode();

                    if (responseCode == 200) {

                        JsonFactory jsonFactory = new JsonFactory();
                        jsonFactory.setCodec(new ObjectMapper());

                        InputStream inputStream = connection.getInputStream();
                        JsonParser jsonParser = jsonFactory.createParser(inputStream);

                        // this will fetch all the available tenants and will then wait until new tenant events occur
                        JsonNode jsonNode = jsonParser.readValueAsTree();

                        // When the connection is closed, the response is null
                        while (jsonNode != null) {
                            JsonNode metaData = jsonNode.get("object").get("metadata");
                            String projectName = metaData.get("name").asText();
                            String projectID = metaData.get("uid").asText();

                            String action = jsonNode.get("type").asText();

                            if (action.equals("ADDED")) {
                                log.debugf("Project %s [%s] has been added.", projectName, projectID);
                                namespaces.put(projectName, projectID);
                            } else if (action.equals("DELETED")) {
                                log.debugf("Project %s [%s] has been removed.", projectName, projectID);
                                namespaces.remove(projectName, projectID);
                            }

                            // this will read the next available tenant if available or wait until the next available
                            // tenant event occurs
                            jsonNode = jsonParser.readValueAsTree();
                        }

                        jsonParser.close();
                        inputStream.close();
                        namespaces.clear();

                    } else {
                        log.warnf("Error getting metadata. Response code %s", responseCode);
                        try {
                            // try again after a second
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            log.error(e);
                            stop();
                        }
                    }

                } catch (Exception e) {
                    log.error(e);
                    try {
                        // try again after a second
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        log.error(ie);
                        stop();
                    }
                }
            }
        }
    }
}
