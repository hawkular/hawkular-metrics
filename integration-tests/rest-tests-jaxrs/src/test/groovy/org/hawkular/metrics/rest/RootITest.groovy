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

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assume.assumeTrue

import org.junit.Test

/**
 * @author mwringe
 */
class RootITest extends RESTTest {

    @Test
    void getServiceInformation() {
        def version = System.properties.getProperty('project.version');
        assumeTrue('This test only works in a Maven build', version != null)

        def response = hawkularMetrics.get(path: '');
        assertEquals(200, response.status)

        assertEquals('Hawkular-Metrics', response.data.name)
        assertEquals(version, response.data['Implementation-Version'])
        assertNotNull(response.data['Built-From-Git-SHA1'])
        assertNotEquals('Unknown', response.data['Built-From-Git-SHA1'])
    }
}
