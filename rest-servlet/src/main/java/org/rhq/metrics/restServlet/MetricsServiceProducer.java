/*
 * Copyright 2014-2015 Red Hat, Inc.
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
package org.rhq.metrics.restServlet;

import static org.rhq.metrics.restServlet.config.ConfigurationKey.BACKEND;
import static org.rhq.metrics.restServlet.config.ConfigurationKey.CASSANDRA_CQL_PORT;
import static org.rhq.metrics.restServlet.config.ConfigurationKey.CASSANDRA_KEYSPACE;
import static org.rhq.metrics.restServlet.config.ConfigurationKey.CASSANDRA_NODES;

import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.RHQMetrics;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.restServlet.config.Configurable;
import org.rhq.metrics.restServlet.config.ConfigurationProperty;

/**
 * @author John Sanda
 */
@ApplicationScoped
public class MetricsServiceProducer {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsServiceProducer.class);

    @Inject
    @Configurable
    @ConfigurationProperty(BACKEND)
    private String backend;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_CQL_PORT)
    private String cqlPort;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_NODES)
    private String nodes;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_KEYSPACE)
    private String keyspace;

    private MetricsService metricsService;

    @Produces
    public MetricsService getMetricsService() {
        if (metricsService == null) {
            RHQMetrics.Builder metricsServiceBuilder = new RHQMetrics.Builder();

            if (backend != null) {
                switch (backend) {
                case "cass":
                    LOG.info("Using Cassandra backend implementation");
                    Map<String, String> options = new HashMap<>();
                    options.put("cqlport", cqlPort);
                    options.put("nodes", nodes);
                    options.put("keyspace", keyspace);
                    metricsServiceBuilder.withOptions(options).withCassandraDataStore();
                    break;
                case "mem":
                    LOG.info("Using memory backend implementation");
                    metricsServiceBuilder.withInMemoryDataStore();
                    break;
                case "embedded_cass":
                default:
                    LOG.info("Using Cassandra backend implementation with an embedded Server");
                    metricsServiceBuilder.withCassandraDataStore();
                }
            } else {
                metricsServiceBuilder.withCassandraDataStore();
            }

            metricsService = metricsServiceBuilder.build();
            ServiceKeeper.getInstance().service = metricsService;
        }

        return metricsService;
    }
}
