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
package org.hawkular.metrics.benchmark.jmh.util;

import static org.scassandra.http.client.PrimingRequest.PrimingRequestBuilder;
import static org.scassandra.http.client.PrimingRequest.then;

import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

/**
 * ClusterManager that maintains the Scassandra implementation. Add additional statements to primeStatements if you
 * wish to test additional features (or behavior change).
 *
 * @author Michael Burman
 */
public class SCassandraManager implements ClusterManager {

    private Scassandra scassandra;
    private PrimingClient primingClient;
    private ActivityClient activityClient;

    private Session session;
    private Cluster cluster;

    public SCassandraManager() {
    }

    @Override
    public void startCluster() {
        scassandra = ScassandraFactory.createServer();
        scassandra.start();
        primingClient = scassandra.primingClient();
        activityClient = scassandra.activityClient();

        primeStatements();
    }

    public Session createSession() {
        if(cluster == null || session == null) {
            cluster = Cluster.builder()
                    .addContactPoint("localhost")
                    .withPort(8042)
                    .build();
            session = cluster.connect("scassandra");
        }
        return session;
    }

    // Selected queries copied from DataAccessImpl
    private void primeStatements() {
        PrimingRequestBuilder findAllTenantIds = PrimingRequest.preparedStatementBuilder()
                .withQuery("SELECT DISTINCT id FROM tenants")
                .withThen(then()
                        .withRows(ImmutableMap.of("id", "hawkular")));
        primingClient.prime(findAllTenantIds);

        PrimingRequestBuilder findAllTenantIdsFromMetricsIdx = PrimingRequest.preparedStatementBuilder()
                .withQuery("SELECT DISTINCT tenant_id, type FROM metrics_idx");

        primingClient.prime(findAllTenantIdsFromMetricsIdx);
    }

    public void shutdown() {
        session.close();
        cluster.close();
        scassandra.stop();
    }
}
