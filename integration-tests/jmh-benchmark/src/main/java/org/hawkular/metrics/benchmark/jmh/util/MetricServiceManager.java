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

import org.hawkular.metrics.core.dropwizard.HawkularMetricRegistry;
import org.hawkular.metrics.core.dropwizard.MetricNameService;
import org.hawkular.metrics.core.service.DataAccessImpl;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.schema.SchemaService;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;

import com.datastax.driver.core.Session;

/**
 * MetricsService lifecycle management, influenced by the MetricsServiceLifecycle from JAX-RS module
 *
 * @author Michael Burman
 */
public class MetricServiceManager {

    private static final int DEFAULT_TTL = 7;
    private MetricsServiceImpl metricsService;
    private ClusterManager manager;
    private String keyspace;
    private Session session;

    public MetricServiceManager(ClusterManager manager) {
        this.manager = manager;
        manager.startCluster();
        session = manager.createSession();
        keyspace = System.getProperty("hawkular.metrics.cassandra.keyspace", "benchmark");
        metricsService = createMetricsService(session);
    }

    private MetricsServiceImpl createMetricsService(Session session) {
        SchemaService schemaService = new SchemaService();
        schemaService.run(session, keyspace, true);

        ConfigurationService configurationService = new ConfigurationService();
        configurationService.init(new RxSessionImpl(session));

        selectKeyspace(session);

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(new DataAccessImpl(session));
        metricsService.setConfigurationService(configurationService);
        metricsService.setDefaultTTL(DEFAULT_TTL);

        HawkularMetricRegistry metricRegistry = new HawkularMetricRegistry();
        metricRegistry.setMetricNameService(new MetricNameService());

        metricsService.startUp(session, keyspace, false, false, metricRegistry);

        return metricsService;
    }

    private void selectKeyspace(Session session) {
        session.execute("USE " + keyspace);
    }

    public MetricsServiceImpl getMetricsService() {
        return metricsService;
    }

    public Session getSession() {
        return session;
    }

    public void shutdown() {
        metricsService.shutdown();
        manager.shutdown();
    }
}
