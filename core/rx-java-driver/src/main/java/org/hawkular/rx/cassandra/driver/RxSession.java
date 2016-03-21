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
package org.hawkular.rx.cassandra.driver;

import java.io.Closeable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * An RxJava flavor of the DataStax's Java driver {@link Session} interface.
 *
 * @author jsanda
 */
public interface RxSession extends Closeable {

    /**
     * @see Session#getLoggedKeyspace()
     */
    String getLoggedKeyspace();

    /**
     * @see Session#init()
     */
    RxSession init();

    /**
     * Asynchronously execute a query and emit the corresponding {@link ResultSet} on the
     * {@link Schedulers#computation()} scheduler.
     *
     * @param query the CQL query to execute
     *
     * @return an {@link Observable} emitting just one {@link ResultSet} item
     */
    Observable<ResultSet> execute(String query);

    /**
     * Asynchronously execute a query and fetch {@link Row}s, emitted on the {@link Schedulers#computation()}
     * scheduler.
     *
     * @param query the CQL query to execute
     *
     * @return an {@link Observable} emitting {@link Row} items
     */
    Observable<Row> executeAndFetch(String query);

    /**
     * Asynchronously execute a query and emit the corresponding {@link ResultSet} on the specified {@code scheduler}.
     *
     * @param query     the CQL query to execute
     * @param scheduler the {@link Scheduler} on which the returned {@link Observable} operates
     *
     * @return an {@link Observable} emitting just one {@link ResultSet} item
     */
    Observable<ResultSet> execute(String query, Scheduler scheduler);

    /**
     * Asynchronously execute a query and fetch {@link Row}s, emitted on the specified {@code scheduler}.
     *
     * @param query     the CQL query to execute
     * @param scheduler the {@link Scheduler} on which the returned {@link Observable} operates
     *
     * @return an {@link Observable} emitting {@link Row} items
     */
    Observable<Row> executeAndFetch(String query, Scheduler scheduler);

    /**
     * Asynchronously execute a query and emit the corresponding {@link ResultSet} on the
     * {@link Schedulers#computation()} scheduler, using the provided {@code values}.
     *
     * @param query  the CQL query to execute
     * @param values values required for the execution of the query
     *
     * @return an {@link Observable} emitting just one {@link ResultSet} item
     */
    Observable<ResultSet> execute(String query, Object... values);

    /**
     * Asynchronously execute a query and fetch {@link Row}s, emitted on the {@link Schedulers#computation()}
     * scheduler, using the provided {@code values}.
     *
     * @param query  the CQL query to execute
     * @param values values required for the execution of the query
     *
     * @return an {@link Observable} emitting {@link Row} items
     */
    Observable<Row> executeAndFetch(String query, Object... values);

    /**
     * Asynchronously execute a query and emit the corresponding {@link ResultSet} on the specified {@code scheduler},
     * using the proved {@code values}.
     *
     * @param query     the CQL query to execute
     * @param scheduler the {@link Scheduler} on which the returned {@link Observable} operates
     * @param values    values required for the execution of the query
     *
     * @return an {@link Observable} emitting just one {@link ResultSet} item
     */
    Observable<ResultSet> execute(String query, Scheduler scheduler, Object... values);

    /**
     * Asynchronously execute a query and fetch {@link Row}s, emitted on the specified {@code scheduler}, using the
     * provided {@code values}.
     *
     * @param query     the CQL query to execute
     * @param scheduler the {@link Scheduler} on which the returned {@link Observable} operates
     * @param values    values required for the execution of the query
     *
     * @return an {@link Observable} emitting {@link Row} items
     */
    Observable<Row> executeAndFetch(String query, Scheduler scheduler, Object... values);

    /**
     * Asynchronously execute a query and emit the corresponding {@link ResultSet} on the
     * {@link Schedulers#computation()} scheduler.
     *
     * @param statement the CQL query to execute, of any {@link Statement} type
     *
     * @return an {@link Observable} emitting just one {@link ResultSet} item
     */
    Observable<ResultSet> execute(Statement statement);

    /**
     * Asynchronously execute a query and fetch {@link Row}s, emitted on the {@link Schedulers#computation()}
     * scheduler.
     *
     * @param statement the CQL query to execute, of any {@link Statement} type
     *
     * @return an {@link Observable} emitting {@link Row} items
     */
    Observable<Row> executeAndFetch(Statement statement);

    /**
     * Asynchronously execute a query and emit the corresponding {@link ResultSet} on the specified {@code scheduler}.
     *
     * @param statement the CQL query to execute, of any {@link Statement} type
     * @param scheduler the {@link Scheduler} on which the returned {@link Observable} operates
     *
     * @return an {@link Observable} emitting just one {@link ResultSet} item
     */
    Observable<ResultSet> execute(Statement statement, Scheduler scheduler);

    /**
     * Asynchronously execute a query and fetch {@link Row}s, emitted on the specified {@code scheduler}.
     *
     * @param statement the CQL query to execute, of any {@link Statement} type
     * @param scheduler the {@link Scheduler} on which the returned {@link Observable} operates
     *
     * @return an {@link Observable} emitting {@link Row} items
     */
    Observable<Row> executeAndFetch(Statement statement, Scheduler scheduler);

    /**
     * Asynchronously prepare a query and emit the corresponding {@link PreparedStatement} on the
     * {@link Schedulers#computation()} scheduler.
     *
     * @param query the CQL query to prepare
     *
     * @return an {@link Observable} emitting just one {@link PreparedStatement} item
     */
    Observable<PreparedStatement> prepare(String query);

    /**
     * Asynchronously prepare a query and emit the corresponding {@link PreparedStatement} on the specified
     * {@code scheduler}.
     *
     * @param query the CQL query to prepare
     *
     * @return an {@link Observable} emitting just one {@link PreparedStatement} item
     */
    Observable<PreparedStatement> prepare(String query, Scheduler scheduler);

    /**
     * Asynchronously prepare a query and emit the corresponding {@link PreparedStatement} on the
     * {@link Schedulers#computation()} scheduler.
     *
     * @param statement the {@link RegularStatement} to prepare
     *
     * @return an {@link Observable} emitting just one {@link PreparedStatement} item
     * @see Session#prepareAsync(RegularStatement)
     */
    Observable<PreparedStatement> prepare(RegularStatement statement);

    /**
     * Asynchronously prepare a query and emit the corresponding {@link PreparedStatement} on the specified
     * {@code scheduler}.
     *
     * @param statement the {@link RegularStatement} to prepare
     *
     * @return an {@link Observable} emitting just one {@link PreparedStatement} item
     * @see Session#prepareAsync(RegularStatement)
     */
    Observable<PreparedStatement> prepare(RegularStatement statement, Scheduler scheduler);

    /**
     * @see Session#close()
     */
    void close();

    /**
     * @see Session#isClosed()
     */
    boolean isClosed();

    /**
     * @see Session#getCluster()
     */
    Cluster getCluster();

    /**
     * @return the underlying {@link Session} object
     */
    Session getSession();

    /**
     * @see Session#getState()
     */
    Session.State getState();

}
