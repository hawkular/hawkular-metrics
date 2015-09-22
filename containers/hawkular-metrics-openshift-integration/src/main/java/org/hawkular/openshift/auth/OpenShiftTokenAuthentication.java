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


package org.hawkular.openshift.auth;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mwringe
 */
public class OpenShiftTokenAuthentication {

    private static final Logger logger = LoggerFactory.getLogger(OpenShiftTokenAuthentication.class);

    private static final String HAWKULAR_TENANTS = "Hawkular-Tenants";

    private static final String KUBERNETES_MASTER_URL =
            System.getProperty("KUBERNETES_MASTER_URL", "https://kubernetes.default.svc.cluster.local");

    //The resource to check against for security purposes. For this version we are allowing Metrics based on a users
    //access to the pods in in a particular project.
    private static final String RESOURCE = "pods";

    private static final String KIND = "LocalSubjectAccessReview";

    enum HTTP_METHOD {GET, PUT, POST, DELETE, PATCH};

    public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws
            IOException, ServletException {

        String token = request.getHeader(OpenShiftAuthenticationFilter.AUTHORIZATION_HEADER);
        String tenant = request.getHeader(HAWKULAR_TENANTS);

        if (token == null || tenant == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                               "The '" + OpenShiftAuthenticationFilter.AUTHORIZATION_HEADER + "' and '"
                                       + HAWKULAR_TENANTS + "' headers" + " are required");
            return;
        }

        //TODO: remove this once we have Tenants working properly in Heapster
        tenant = "default";

        if (isAuthorized(request.getMethod(), token, tenant)) {
            filterChain.doFilter(request, response);
            return;
        } else {
            response.sendError(HttpServletResponse.SC_FORBIDDEN);
            return;
        }
    }

    private boolean isAuthorized(String method, String token, String projectId) {
        try {

            String verb = getVerb(method);

            String path = "/oapi/v1/namespaces/" + projectId + "/subjectaccessreviews";
            URL url = new URL(KUBERNETES_MASTER_URL + path);

            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();

            //Configure the outgoing request
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            //Set Headers
            connection.setRequestProperty("Accept", "application/json");
            connection.addRequestProperty("Authorization", token);

            //Add the body
            OutputStream outputStream = connection.getOutputStream();
            for (byte b : generateSubjectAccessReview(verb).getBytes()) {
                outputStream.write(b);
            }
            outputStream.flush();
            outputStream.close();

            //Perform the Operation
            connection.connect();
            int responseCode = connection.getResponseCode();
            if (responseCode == 201) {
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(connection.getInputStream());

                if (jsonNode.get("allowed").asText().equals("true")) {
                    return true;
                }

            } else {
                return false;
            }

        } catch (IOException e) {
            logger.error("Error trying to authenticate against the OpenShift server", e);
        }

        return false;
    }


    /**
     *
     * Generates a SubjectAccessReview object used to request if a user has a certain permission or not.
     *
     * @param verb
     * @return
     * @throws IOException
     */
    private String generateSubjectAccessReview(String verb) throws IOException {
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        objectNode.put("kind", KIND);
        objectNode.put("resource", RESOURCE);
        objectNode.put("verb", verb);
        return objectNode.toString();
    }

    /**
     * Determine the verb we should apply based on the HTTP method being requested.
     *
     * In this case, an HTTP GET will require the 'get' permission in OpenShift
     *
     * HTTP PUT, POST & PATCH will require the 'update' permission in OpenShift
     *
     * HTTP DELETE will require the 'delete' permission in OpenShift
     *
     * @param method
     * @return The verb to use or null if no verb could be determined
     */
    private String getVerb(String method) {

        HTTP_METHOD httpMethod = HTTP_METHOD.valueOf(method);
        switch (httpMethod) {
            case GET:
                return "list";
            case PUT:
                return "update";
            case POST:
                return "update";
            case DELETE:
                return "delete";
            case PATCH:
                return "update";
        }

        return null;
    }

}
