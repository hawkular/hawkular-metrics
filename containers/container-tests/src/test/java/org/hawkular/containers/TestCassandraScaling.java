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

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.ReplicationController;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author mwringe
 */
@RunWith(Arquillian.class)
public class TestCassandraScaling extends BaseContainerTests {


    @Test
    public void testScaling() throws Exception {
        // Test with the intial setup of 1 replica
        testWriteData("initial", "11.102", 1);
        ReplicationController rc = client.getReplicationController(CASSANDRA_NAME);

        // Test scaling up to 2 replicas
        rc.getSpec().setReplicas(2);
        client.updateReplicationController(CASSANDRA_NAME, rc);

        while(client.getPodsForService(CASSANDRA_NAME).size() < 2) {
            Thread.sleep(1000);
        }

        testWriteData("2reps", "22.42", 2);

        // Test scaling down to 1 replica
        rc = client.getReplicationController(CASSANDRA_NAME);
        rc.getSpec().setReplicas(1);
        client.updateReplicationController(CASSANDRA_NAME, rc);

        // TODO: check that the docker images have been shut down and not just removed from the service
        while (client.getPodsForService(CASSANDRA_NAME).size() > 1) {
            Thread.sleep(1000);
        }

        testWriteData("1rep","13.3", 3);
    }

    //@Test
    public void testWriteData(String metricId, String value, int timestamp) throws Exception {
        JsonNode json = getJSON("/hawkular/metrics/gauges/" + metricId + "/data");
        assertEquals(null, json);

        sendMetric(metricId, value, timestamp);

        json = getJSON("/hawkular/metrics/gauges/" + metricId + "/data");

        String expected = "[{\"timestamp\":" + timestamp + ",\"value\":" + value + "}]";

        assertEquals(expected, json.toString());
    }

}
