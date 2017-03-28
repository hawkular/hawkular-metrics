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
package org.hawkular.metrics.alerting

import org.junit.AfterClass
import org.junit.BeforeClass

import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient
import groovyx.net.http.HttpResponseDecorator
import groovyx.net.http.HttpResponseException

/**
 * Base class for REST tests.
 *
 * @author Lucas Ponce
 */
class AlertingITestBase {

    static baseURI = System.getProperty('hawkular-alerts.base-uri') ?: 'http://127.0.0.1:8080/hawkular/alerts'
    static metricsBaseURI = System.getProperty('hawkular-metrics.base-uri') ?: 'http://127.0.0.1:8080/hawkular/metrics'
    static RESTClient client
    static RESTClient metricsClient
    static String tenantId = "hawkular"

    @BeforeClass
    static void initClient() {
        client = new RESTClient("http://$baseURI/", ContentType.JSON)
        // this prevents 404 from being wrapped in an Exception, just return the response, better for testing
        client.handler.failure = { it }

        metricsClient = new RESTClient("http://$metricsBaseURI/", ContentType.JSON)
        metricsClient.handler.failure = { it }

        /*
            User: jdoe
            Password: password
            String encodedCredentials = Base64.getMimeEncoder()
            .encodeToString("$testUser:$testPasword".getBytes("utf-8"))
         */
        // client.defaultRequestHeaders.Authorization = "Basic amRvZTpwYXNzd29yZA=="
        client.defaultRequestHeaders.'Hawkular-Tenant' = tenantId
        // metricsClient.defaultRequestHeaders.Authorization = "Basic amRvZTpwYXNzd29yZA=="
        metricsClient.defaultRequestHeaders.'Hawkular-Tenant' = tenantId
    }
}
