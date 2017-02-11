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
package org.hawkular.metrics.core.service.transformers;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Statement;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func0;

/**
 * Groups {@link Statement} items into {@link BatchStatement} items.
 *
 * @author Thomas Segismont
 */
public class BoundBatchStatementTransformer implements Transformer<BoundStatement, BatchStatement> {
    public static final int DEFAULT_BATCH_SIZE = 50;

    /**
     * Creates {@link BatchStatement.Type#UNLOGGED} batch statements.
     */
    public static final Func0<BatchStatement> DEFAULT_BATCH_STATEMENT_FACTORY = () -> new BatchStatement(UNLOGGED);

    private final Func0<BatchStatement> batchStatementFactory;
    private final int batchSize;

    /**
     * Creates a new transformer using the {@link #DEFAULT_BATCH_STATEMENT_FACTORY}.
     */
    public BoundBatchStatementTransformer() {
        this(DEFAULT_BATCH_STATEMENT_FACTORY, DEFAULT_BATCH_SIZE);
    }

    /**
     * @param batchStatementFactory function used to initialize a new {@link BatchStatement}
     * @param batchSize             maximum number of statements in the batch
     */
    public BoundBatchStatementTransformer(Func0<BatchStatement> batchStatementFactory, int batchSize) {
        this.batchSize = batchSize;
//        checkArgument(batchSize <= DEFAULT_BATCH_SIZE, "batchSize exceeds limit");
        this.batchStatementFactory = batchStatementFactory;
    }

    @Override
    public Observable<BatchStatement> call(Observable<BoundStatement> statements) {
        return statements
                .window(batchSize)
                .flatMap(window -> window.collect(batchStatementFactory, BatchStatement::add));
    }
}
