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

import org.junit.Test

import static org.junit.Assert.*
import static org.junit.Assume.assumeTrue
/**
 * @author mwringe
 */
class StatusITest extends RESTTest {
    @Test
    void getStatus() {
        def version = System.properties.getProperty('project.version');
        assumeTrue('This test only works in a Maven build', version != null)

        def response = hawkularMetrics.get(path: 'status')
        assertEquals(200, response.status)

        def expectedState = ['MetricsService': 'STARTED']

        assertEquals(expectedState.MetricsService, response.data.MetricsService)
        assertEquals(version, response.data['Implementation-Version'])
        assertNotNull(response.data['Built-From-Git-SHA1'])
        assertNotEquals('Unknown', response.data['Built-From-Git-SHA1'])
    }
}