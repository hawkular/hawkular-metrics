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

import static io.fabric8.kubernetes.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.ReplicationController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import java.util.List;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author mwringe
 */
@RunWith(Arquillian.class)
public class TestContainers extends BaseContainerTests{

    @Test
    public void testMetricsRC() throws Exception {
        ReplicationController rc = client.getReplicationController(HAWKULAR_METRICS_NAME);
        assertThat(rc).isNotNull();

        assertThat(rc.getSpec()).hasReplicas(1);
        assertThat(rc.getStatus()).hasReplicas(1);
    }

    @Test
    public void testCassandraRC() throws Exception {
        ReplicationController rc = client.getReplicationController(CASSANDRA_NAME);
        assertThat(rc).isNotNull();
        assertThat(rc.getSpec()).hasReplicas(1);
        assertThat(rc.getStatus()).hasReplicas(1);
    }

    @Test
    public void testMetricsService() throws Exception {
        Service service = client.getService(HAWKULAR_METRICS_NAME);
        assertThat(service).isNotNull();

        ServiceSpec spec = service.getSpec();
        assertThat(spec).isNotNull();
        assertThat((List)spec.getPorts()).hasSize(1);
        ServicePort port = spec.getPorts().get(0);
        assertThat(port).hasPort(80);
    }

    @Test
    public void testCassandraService() throws Exception {
        Service service = client.getService(CASSANDRA_NAME);
        assertThat(service).isNotNull();

        ServiceSpec spec = service.getSpec();
        assertTrue(!spec.getPortalIP().equalsIgnoreCase("None"));
        assertThat(spec).isNotNull();

        assertTrue(checkPorts(spec.getPorts(), 9042, 9160, 7000, 7001));
    }

    @Test
    public void testCassandraClusterService() throws Exception {
        Service service = client.getService(CASSANDRA_CLUSTER_NAME);
        assertThat(service).isNotNull();

        ServiceSpec spec = service.getSpec();
        assertThat(spec).hasPortalIP("None");
        assertThat(spec).isNotNull();

        assertTrue(checkPorts(spec.getPorts(), 9042, 9160, 7000, 7001));
    }

    @Test
    public void testMetricsPods() throws Exception {
        List<Pod> pods = client.getPodsForService(HAWKULAR_METRICS_NAME);
        assertThat(pods).isNotNull();
        assertThat((List)pods).hasSize(1);

        Pod pod = pods.get(0);
        assertEquals("Ready", pod.getStatus().getConditions().get(0).getType());
    }

    @Test
    public void testCassandraPods() throws Exception {
        List<Pod> pods = client.getPodsForService(CASSANDRA_NAME);
        assertThat(pods).isNotNull();
        assertThat((List)pods).hasSize(1);

        Pod pod = pods.get(0);
        assertEquals(READY_STATE, pod.getStatus().getConditions().get(0).getType());
    }

    @Test
    public void testCassandraClusterPods() throws Exception {
        List<Pod> pods = client.getPodsForService(CASSANDRA_CLUSTER_NAME);
        assertThat(pods).isNotNull();
        assertThat((List)pods).hasSize(1);

        Pod pod = pods.get(0);
        assertEquals(READY_STATE, pod.getStatus().getConditions().get(0).getType());
    }

    private boolean checkPorts(List<ServicePort> ports, int... expectedPorts) {
        if (ports.size() != expectedPorts.length) {
            return false;
        }

        for (int expectedPort: expectedPorts) {
            boolean found = false;
            for (ServicePort servicePort: ports) {
                if (servicePort.getPort() == expectedPort) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }

        return true;
    }
}
