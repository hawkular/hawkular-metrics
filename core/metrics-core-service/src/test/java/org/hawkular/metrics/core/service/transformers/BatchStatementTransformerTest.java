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

package org.hawkular.metrics.core.service.transformers;

import static org.hawkular.metrics.core.service.transformers.BatchStatementTransformer.DEFAULT_BATCH_STATEMENT_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;

import rx.Observable;

/**
 * @author Thomas Segismont
 */
public class BatchStatementTransformerTest {

    private int batchSize;
    private BatchStatementTransformer batchStatementTransformer;

    @Before
    public void setUp() throws Exception {
        batchSize = 5;
        batchStatementTransformer = new BatchStatementTransformer(DEFAULT_BATCH_STATEMENT_FACTORY, batchSize);
    }

    @Test
    public void testCall() throws Exception {
        int expected = 6;
        // Emit enough statements to get expected count of batches, with the last batch holding just one
        List<BatchStatement> result = Observable.range(0, (expected - 1) * batchSize + 1)
                .map(i -> mock(Statement.class))
                .compose(batchStatementTransformer)
                .toList()
                .toBlocking()
                .single();
        assertEquals(expected, result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(i < (result.size() - 1) ? batchSize : 1, result.get(i).size());
        }
    }
}