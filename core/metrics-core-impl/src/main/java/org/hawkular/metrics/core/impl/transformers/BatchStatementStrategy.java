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

import org.jboss.logging.Logger;

import com.datastax.driver.core.BatchStatement;
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

    private final int batchSize;

    /**
     * Creates a new transformer using the {@link #MAX_BATCH_SIZE}.
     */
    public BatchStatementStrategy() {
        this(MAX_BATCH_SIZE);
    }

    /**
     * @param batchSize maximum number of statements in the batch
     */
    public BatchStatementStrategy(int batchSize) {
        if (batchSize <= MAX_BATCH_SIZE) {
            this.batchSize = batchSize;
        } else {
            log.trace("Batch size exceeds limit, will use allowed maximum");
            this.batchSize = MAX_BATCH_SIZE;
        }
    }

    @Override
    public Observable<Statement> call(Observable<Statement> statements) {
        return statements
                .window(batchSize)
                .flatMap(window -> window.collect(() -> new BatchStatement(UNLOGGED), BatchStatement::add));
    }
}
