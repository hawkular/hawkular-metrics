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
package org.hawkular.metrics.core.impl.transformers;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

import org.jboss.logging.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import rx.Observable;

/**
 * Groups {@link Statement} items into <strong>UNLOGGED</strong> {@link BatchStatement} items.
 *
 * @author Thomas Segismont
 */
public class BatchStatementStrategy implements BatchingStrategy {
    private static final Logger log = Logger.getLogger(BatchStatementStrategy.class);

    public static final int MAX_BATCH_SIZE = 10;

    private final Metadata metadata;
    private final boolean groupByReplica;
    private final int batchSize;

    /**
     * Creates a new transformer using the {@link #MAX_BATCH_SIZE}.
     *
     * @param session the C* driver's session
     * @param groupByReplica if true group statements by replica
     */
    public BatchStatementStrategy(Session session, boolean groupByReplica) {
        this(session, groupByReplica, MAX_BATCH_SIZE);
    }

    /**
     * @param session   the C* driver's session
     * @param groupByReplica if true group statements by replica
     * @param batchSize maximum number of statements in the batch
     */
    public BatchStatementStrategy(Session session, boolean groupByReplica, int batchSize) {
        checkArgument(!groupByReplica || session != null, "session is null");
        this.groupByReplica = groupByReplica;
        this.metadata = session == null ? null : session.getCluster().getMetadata();
        if (batchSize <= MAX_BATCH_SIZE) {
            this.batchSize = batchSize;
        } else {
            log.trace("Batch size exceeds limit, will use allowed maximum");
            this.batchSize = MAX_BATCH_SIZE;
        }
    }

    @Override
    public Observable<Statement> call(Observable<Statement> statements) {
        Observable<Observable<Statement>> groups;
        if (groupByReplica) {
            boolean trace = log.isTraceEnabled();
            LongAdder numberOfReplicas = new LongAdder();
            groups = statements.groupBy(statement -> {
                ByteBuffer routingKey = statement.getRoutingKey();
                if (routingKey == null) {
                    return null;
                }
                Set<Host> replicas = metadata.getReplicas(statement.getKeyspace(), routingKey);
                return replicas.isEmpty() ? null : replicas.iterator().next();
            }).flatMap(group -> {
                if (trace && group.getKey() != null) {
                    numberOfReplicas.increment();
                }
                return group.window(batchSize);
            });
            if (trace) {
                groups = groups.doOnCompleted(() -> {
                    log.tracef("Executed batches for %s known replicas", String.valueOf(numberOfReplicas));
                });
            }
        } else {
            groups = statements.window(batchSize);
        }
        return groups.flatMap(window -> window.collect(() -> new BatchStatement(UNLOGGED), BatchStatement::add));
    }
}
