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
package org.hawkular.metrics.test;

import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.junit.BeforeClass;

/**
 * @author Jeeva Kandasamy
 */
public class RESTTest {
    static final String TENANT_HEADER_NAME = "Hawkular-Tenant";

    static String baseURI = System.getProperty("hawkular-metrics.base-uri", "127.0.0.1:8080/hawkular/metrics");
    static ResteasyClient restClient = null;
    static ResteasyWebTarget target = null;

    @BeforeClass
    public static void initClient() {
        if (restClient == null) {
            restClient = new ResteasyClientBuilder().build();
            target = restClient.target("http://" + baseURI);
        }
    }
}
