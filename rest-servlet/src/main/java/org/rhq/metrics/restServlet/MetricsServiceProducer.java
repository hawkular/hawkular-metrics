/*
 * Copyright 2014 Red Hat, Inc.
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

    private MetricsService metricsService;

    @Produces
    public MetricsService getMetricsService() {
        if (metricsService == null) {
            RHQMetrics.Builder metricsServiceBuilder = new RHQMetrics.Builder();

            if (backend != null) {
                switch (backend) {
                    case "cass":
                        LOG.info("Using Cassandra backend implementation");
                        metricsServiceBuilder.withCassandraDataStore();
                        break;
                    case "mem":
                    default:
                        LOG.info("Using memory backend implementation");
                        metricsServiceBuilder.withInMemoryDataStore();
                }
            } else {
                metricsServiceBuilder.withInMemoryDataStore();
            }

            metricsService = metricsServiceBuilder.build();
            ServiceKeeper.getInstance().service = metricsService;
        }

        return metricsService;
    }
}
