/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.rhq.metrics.restServlet;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.rhq.metrics.RHQMetrics;
import org.rhq.metrics.core.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author John Sanda
 */
@ApplicationScoped
public class MetricsServiceProducer {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsServiceProducer.class);

    private MetricsService metricsService;

    @Produces
    public MetricsService getMetricsService() {
        if (metricsService == null) {
            String backend = System.getProperty("rhq-metrics.backend");

            RHQMetrics.Builder metricsServiceBuilder = new RHQMetrics.Builder();

            if (backend != null) {
                switch (backend) {
                case "cass":
                    LOG.info("Using Cassandra backend implementation");
                    metricsServiceBuilder.withCassandraDataStore();
                    break;
                case "mem":
                    LOG.info("Using memory backend implementation");
                    metricsServiceBuilder.withInMemoryDataStore();
                case "embedded_cass":
                default:
                    LOG.info("Using memory backend implementation");
                    metricsServiceBuilder.withEmbeddedCassandraDataStore();
                }
            } else {
                metricsServiceBuilder.withEmbeddedCassandraDataStore();
            }

            metricsService = metricsServiceBuilder.build();
            ServiceKeeper.getInstance().service = metricsService;
        }

        return metricsService;
    }
}
