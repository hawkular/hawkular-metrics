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
package org.hawkular.metrics.api.jaxrs;

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_CQL_PORT;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_KEYSPACE;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_NODES;

import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.impl.CassandraSession;
import org.hawkular.metrics.core.impl.MetricsServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author John Sanda
 */
@ApplicationScoped
@Eager
public class MetricsServiceProducer {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsServiceProducer.class);

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

    @Produces
    private MetricsService metricsService;

    private CassandraSession cassandraSession;

    @PostConstruct
    void init() {
        LOG.info("Initializing metrics service");
        metricsService = new MetricsServiceImpl();
        Map<String, String> options = new HashMap<>();
        options.put("cqlport", cqlPort);
        options.put("nodes", nodes);
        options.put("keyspace", keyspace);

        CassandraSession.Builder cassandraSessionBuilder = new CassandraSession.Builder();
        cassandraSessionBuilder.withOptions(options);

        cassandraSessionBuilder.withInitializationCallback(new FutureCallback<Session>() {
            @Override
            public void onSuccess(Session session) {
                metricsService.startUp(session);
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error("An error occurred trying to connect to the Cassandra cluster.", t);
                metricsService.setState(MetricsService.State.FAILED);
            }
        });

        cassandraSession = cassandraSessionBuilder.build();
    }

    @PreDestroy
    void destroy() {
        metricsService.shutdown();
        cassandraSession.shutdown();
    }
}
