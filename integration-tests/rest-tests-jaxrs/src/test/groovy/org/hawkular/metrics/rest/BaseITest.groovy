/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.rest

import static org.joda.time.DateTime.now
import static org.junit.Assert.assertEquals

import org.junit.Test

class BaseITest extends RESTTest {

    @Test
    void addAndGetValue() {
        def end = now().millis
        def start = end - 100
        def tenantId = 'test-tenant'
        def metric = 'foo'
        def response = hawkularMetrics.post(path: "gauges/$metric/raw", body: [[timestamp: start + 10, value: 42]], headers: [(tenantHeaderName): tenantId])
        assertEquals(200, response.status)

        response = hawkularMetrics.get(path: "gauges/$metric/raw", query: [start: start, end: end], headers: [(tenantHeaderName): tenantId])
        assertEquals(200, response.status)

        assertEquals(start + 10, response.data[0].timestamp)
    }
}
