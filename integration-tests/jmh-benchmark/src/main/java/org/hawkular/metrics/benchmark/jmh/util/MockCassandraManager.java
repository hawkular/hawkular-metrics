/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.benchmark.jmh.util;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.mockito.Mockito;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * ClusterManager based on the idea of Mocking the Session object
 *
 * @author Michael Burman
 */
public class MockCassandraManager implements ClusterManager {

    @Override
    public void startCluster() {

    }

    @Override
    public Session createSession() {
        Session session = mock(Session.class);
        ResultSet result = mock(ResultSet.class);

        List<Row> rows = new ArrayList<>();
        Mockito.doReturn(rows).when(result).all();
        Mockito.doReturn(true).when(result).isFullyFetched();

        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        BoundStatement boundStatement = mock(BoundStatement.class);
        when(session.prepare(any(RegularStatement.class))).thenReturn(preparedStatement);
        when(preparedStatement.bind()).thenReturn(boundStatement);
        when(boundStatement.setString(any(String.class), any(String.class))).thenReturn(boundStatement);

        ResultSetFuture future = mock(ResultSetFuture.class);
        try {
            Mockito.doReturn(result).when(future).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        Mockito.doReturn(future).when(session).executeAsync(Mockito.anyString());
        return session;
    }

    @Override public void shutdown() {

    }
}
