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
package org.hawkular.metrics.benchmark.jmh.util;

import java.util.Arrays;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;

/**
 * Maintains the session and cluster lifecycle of real Cassandra connection.
 *
 * @author Michael Burman
 */
public class LiveCassandraManager implements ClusterManager {

    private Cluster cluster;
    private Session session;
    private String nodes;

    @Override
    public void startCluster() {
        nodes = System.getProperty("hawkular.metrics.cassandra.nodes", "127.0.0.1");
    }

    @Override
    public Session createSession() {
        Cluster.Builder clusterBuilder = new Cluster.Builder()
                .withPort(9042)
                .withoutJMXReporting()
                .withPoolingOptions(new PoolingOptions()
                                .setMaxConnectionsPerHost(HostDistance.LOCAL, 1024)
                                .setCoreConnectionsPerHost(HostDistance.LOCAL, 1024)
                                .setMaxConnectionsPerHost(HostDistance.REMOTE, 1024)
                                .setCoreConnectionsPerHost(HostDistance.REMOTE, 1024)
                                .setMaxRequestsPerConnection(HostDistance.LOCAL, 1024)
                                .setMaxRequestsPerConnection(HostDistance.REMOTE, 1024)
                                .setMaxQueueSize(1024));

        Arrays.stream(nodes.split(",")).forEach(clusterBuilder::addContactPoints);

        cluster = clusterBuilder.build();
        cluster.init();
        try {
            session = cluster.connect("system");
            return session;
        } finally {
            if (session == null) {
                cluster.close();
            }
        }
    }

    @Override
    public void shutdown() {
        session.close();
        cluster.close();
    }
}
