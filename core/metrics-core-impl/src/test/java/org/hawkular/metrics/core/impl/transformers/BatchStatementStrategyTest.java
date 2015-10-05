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

import static java.util.stream.Collectors.toSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableSet;

import rx.Observable;

/**
 * @author Thomas Segismont
 */
public class BatchStatementStrategyTest {

    @Test
    public void testBatchSize() throws Exception {
        int batchSize = 5;
        BatchingStrategy batchStatementStrategy = new BatchStatementStrategy(null, false, batchSize);

        int expected = 6;
        // Emit enough statements to get expected count of batches, with the last batch holding just one
        List<Statement> result = Observable.range(0, (expected - 1) * batchSize + 1)
                .map(i -> mock(Statement.class))
                .compose(batchStatementStrategy)
                .toList()
                .toBlocking()
                .single();
        assertEquals(expected, result.size());
        for (int i = 0; i < result.size(); i++) {
            Statement statement = result.get(i);
            if (statement instanceof BatchStatement) {
                BatchStatement batchStatement = (BatchStatement) statement;
                assertEquals(i < (result.size() - 1) ? batchSize : 1, batchStatement.size());
            } else {
                fail("Not a batch statement: " + statement.getClass().getCanonicalName());
            }
        }
    }

    @Test
    public void shouldGroupByReplica() throws Exception {
        Host[] hosts = new Host[]{mock(Host.class), mock(Host.class), mock(Host.class)};

        Metadata metadata = mock(Metadata.class);
        when(metadata.getReplicas(anyString(), any(ByteBuffer.class))).then(invocation -> {
            ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[1];
            if (buffer == null) {
                return ImmutableSet.of();
            }
            byte index = buffer.get(0);
            if (index < 0 || index > hosts.length - 1) {
                return ImmutableSet.of();
            }
            return ImmutableSet.of(hosts[index]);
        });

        Cluster cluster = mock(Cluster.class);
        when(cluster.getMetadata()).thenReturn(metadata);
        Session session = mock(Session.class);
        when(session.getCluster()).thenReturn(cluster);

        int batchSize = 5;
        BatchingStrategy batchStatementStrategy = new BatchStatementStrategy(session, true, batchSize);

        int expectedBatches = 6;
        List<Statement> result = Observable.range(0, (hosts.length + 2) * batchSize * expectedBatches)
                .map(i -> {
                    Statement mock = mock(Statement.class);
                    int index = i % (hosts.length + 2) - 1;
                    ByteBuffer routingKey = ByteBuffer.wrap(new byte[]{(byte) (index)});
                    when(mock.getRoutingKey()).thenReturn(index == -1 ? null : routingKey);
                    return mock;
                })
                .compose(batchStatementStrategy)
                .toList()
                .toBlocking()
                .single();
        assertEquals((hosts.length + 2) * expectedBatches, result.size());
        for (Statement statement : result) {
            if (statement instanceof BatchStatement) {
                BatchStatement batchStatement = (BatchStatement) statement;
                assertEquals(batchSize, batchStatement.size());
                Set<ByteBuffer> routingKeys = batchStatement.getStatements().stream()
                        .map(Statement::getRoutingKey)
                        .collect(toSet());
                if (routingKeys.size() != 1) {
                    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{(byte) hosts.length});
                    assertTrue(routingKeys.contains(null) && routingKeys.contains(byteBuffer));
                }
            } else {
                fail("Not a batch statement: " + statement.getClass().getCanonicalName());
            }
        }
    }
}