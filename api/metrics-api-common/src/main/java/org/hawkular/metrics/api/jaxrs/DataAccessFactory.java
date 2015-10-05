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

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.STATEMENT_BATCHING_STRATEGY;
import static org.hawkular.metrics.core.impl.transformers.BatchStatementStrategy.MAX_BATCH_SIZE;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationKey;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.core.impl.DataAccess;
import org.hawkular.metrics.core.impl.DataAccessImpl;
import org.hawkular.metrics.core.impl.transformers.BatchStatementStrategy;
import org.hawkular.metrics.core.impl.transformers.NoBatchStatementStrategy;
import org.jboss.logging.Logger;

import com.datastax.driver.core.Session;

/**
 * @author Thomas Segismont
 */
@ApplicationScoped
public class DataAccessFactory {
    private Logger log = Logger.getLogger(DataAccessFactory.class);

    @Inject
    @Configurable
    @ConfigurationProperty(STATEMENT_BATCHING_STRATEGY)
    private String statementBatchingStrategy;

    @Inject
    @Configurable
    @ConfigurationProperty(ConfigurationKey.BATCH_STATEMENT_SIZE)
    private String batchStatementSize;

    @Inject
    @Configurable
    @ConfigurationProperty(ConfigurationKey.BATCH_GROUPBY_REPLICA)
    private String batchGroupByReplica;

    public DataAccess create(Session session) {
        if (statementBatchingStrategy != null) {
            String strategy = statementBatchingStrategy.trim();
            if (NoBatchStatementStrategy.class.getCanonicalName().equals(strategy)) {
                log.debugf("Using %s", NoBatchStatementStrategy.class.getCanonicalName());
                return new DataAccessImpl(session, s -> new NoBatchStatementStrategy());
            }
        }
        return createForBatchStatementStrategy(session);
    }

    private DataAccessImpl createForBatchStatementStrategy(Session session) {
        boolean groupByReplica = batchGroupByReplica != null;
        if (groupByReplica) {
            log.debug("Will group statements by replica");
        }
        if (batchStatementSize != null) {
            String size = batchStatementSize.trim();
            try {
                int batchSize = Integer.parseInt(size);
                if (batchSize <= MAX_BATCH_SIZE) {
                    log.debugf("Using %s with batch size of %s", BatchStatementStrategy.class.getCanonicalName(),
                            String.valueOf(batchSize));
                    return new DataAccessImpl(session, s -> new BatchStatementStrategy(s, groupByReplica, batchSize));
                } else {
                    log.debug("Batch statement size is too big, will use the default");
                }
            } catch (NumberFormatException e) {
                log.debug("Invalid batch statement size, will use the default");
            }
        }
        log.debugf("Using %s with default batch size", BatchStatementStrategy.class.getCanonicalName());
        return new DataAccessImpl(session, s -> new BatchStatementStrategy(s, groupByReplica));
    }
}
