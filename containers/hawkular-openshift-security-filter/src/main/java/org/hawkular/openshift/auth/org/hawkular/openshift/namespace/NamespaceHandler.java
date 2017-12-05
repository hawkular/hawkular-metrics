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

import static io.undertow.util.StatusCodes.NOT_FOUND;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.StringJoiner;

import org.hawkular.openshift.auth.Utils;
import org.jboss.logging.Logger;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;

/**
 *
 */
public class NamespaceHandler implements HttpHandler {

    private static final Logger log = Logger.getLogger(NamespaceHandler.class);

    private final HttpHandler containerHandler;

    //
    private static final String HAWKULAR_TENANT_HEADER_NAME = "Hawkular-Tenant";
    private static final String TOKEN_FILE_LOCATION = "/var/run/secrets/kubernetes.io/serviceaccount/token";

    private static final String KUBERNETES_MASTER_URL_SYSPROP = "KUBERNETES_MASTER_URL";
    private static final String KUBERNETES_MASTER_URL_DEFAULT = "https://kubernetes.default.svc.cluster.local";
    static final String KUBERNETES_MASTER_URL =
            System.getProperty(KUBERNETES_MASTER_URL_SYSPROP, KUBERNETES_MASTER_URL_DEFAULT);
    //

    private final String token;

    private final NamespaceListener namespaceListener;

    private final Map<String,String> projectOverwrites;

    public NamespaceHandler(HttpHandler containerHandler) {
        log.info("Creating NamespaceHandler");
        this.containerHandler = containerHandler;

        try {
            this.token = getToken();
            log.info("Received token from " + TOKEN_FILE_LOCATION);
        } catch (Exception e){
            log.error("Error retrieving token from file " + TOKEN_FILE_LOCATION);
            throw new RuntimeException(e);
        }

        namespaceListener = new NamespaceListener(token);
        NamespaceOverrideMapper nom = new NamespaceOverrideMapper(token);
        log.info("Received namespace overrides: " + nom.overrides);

        this.projectOverwrites = nom.overrides;
    }

    private String getToken() throws IOException {
        return new String(Files.readAllBytes(Paths.get(TOKEN_FILE_LOCATION)));
    }

    @Override
    public void handleRequest(HttpServerExchange serverExchange) throws Exception {
        String originalTenantName = getTenantName(serverExchange);
        if (originalTenantName == null || originalTenantName.startsWith("_") || originalTenantName.contains(":")) {
            containerHandler.handleRequest(serverExchange);
        } else {

            StringJoiner namespaceName = new StringJoiner(",");

            for (String namespace: originalTenantName.split(",")) {
                String namespaceID = namespaceListener.getNamespaceId(namespace);

                if (namespaceID != null) {
                    String tenantName;

                    if (projectOverwrites.containsKey(namespaceID)) {
                        tenantName = projectOverwrites.get(namespaceID);
                    } else {
                        tenantName = namespace + ":" + namespaceID;
                    }
                    namespaceName.add(tenantName);

                } else {
                    log.debug("Could not determine a namespace id for namespace. Cannot process request. Returning NOT_FOUND.");
                    Utils.endExchange(serverExchange, NOT_FOUND, "Could not determine a namespace id for namespace " + namespaceID);
                    return;
                }
            }

            serverExchange.getRequestHeaders().remove(HAWKULAR_TENANT_HEADER_NAME);
            serverExchange.getRequestHeaders().put(new HttpString(HAWKULAR_TENANT_HEADER_NAME), namespaceName.toString());

            containerHandler.handleRequest(serverExchange);
        }
    }

    private String getTenantName(HttpServerExchange httpServerExchange) {
        String tenantName = httpServerExchange.getRequestHeaders().getFirst(HAWKULAR_TENANT_HEADER_NAME);
        return tenantName;
    }

    public void stop() {
        if (namespaceListener != null) {
            namespaceListener.stop();
        }
    }
}
