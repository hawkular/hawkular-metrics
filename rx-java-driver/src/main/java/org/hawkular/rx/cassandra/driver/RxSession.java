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
package org.hawkular.rx.cassandra.driver;

import java.io.Closeable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import rx.Observable;
import rx.Scheduler;

/**
 * @author jsanda
 */
public interface RxSession extends Closeable {

    String getLoggedKeyspace();

    RxSession init();

    Observable<ResultSet> execute(String query);

    Observable<ResultSet> execute(String query, Scheduler scheduler);

    Observable<ResultSet> execute(String query, Object... values);

    Observable<ResultSet> execute(String query, Scheduler scheduler, Object... values);

    Observable<ResultSet> execute(Statement statement);

    Observable<ResultSet> execute(Statement statement, Scheduler scheduler);

    Observable<PreparedStatement> prepare(String query);

    Observable<PreparedStatement> prepare(String query, Scheduler scheduler);

    Observable<PreparedStatement> prepare(RegularStatement statement);

    Observable<PreparedStatement> prepare(RegularStatement statement, Scheduler scheduler);

    void close();

    boolean isClosed();

    Cluster getCluster();

    Session getSession();

    Session.State getState();

}
