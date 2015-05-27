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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * @author jsanda
 */
public class RxSessionImpl implements RxSession {

    private Session session;

    public RxSessionImpl(Session session) {
        this.session = session;
    }

    @Override
    public String getLoggedKeyspace() {
        return session.getLoggedKeyspace();
    }

    @Override
    public RxSession init() {
        session.init();
        return this;
    }

    @Override
    public Observable<ResultSet> execute(String query) {
        ResultSetFuture future = session.executeAsync(query);
        return RxUtil.from(future, Schedulers.computation());
    }

    @Override
    public Observable<ResultSet> execute(String query, Scheduler scheduler) {
        ResultSetFuture future = session.executeAsync(query);
        return RxUtil.from(future, scheduler);
    }

    @Override
    public Observable<ResultSet> execute(String query, Object... values) {
        ResultSetFuture future = session.executeAsync(query, values);
        return RxUtil.from(future, Schedulers.computation());
    }

    @Override
    public Observable<ResultSet> execute(String query, Scheduler scheduler, Object... values) {
        ResultSetFuture future = session.executeAsync(query, values, scheduler);
        return RxUtil.from(future, scheduler);
    }

    @Override
    public Observable<ResultSet> execute(Statement statement) {
        ResultSetFuture future = session.executeAsync(statement);
        return RxUtil.from(future, Schedulers.computation());
    }

    @Override
    public Observable<ResultSet> execute(Statement statement, Scheduler scheduler) {
        ResultSetFuture future = session.executeAsync(statement);
        return RxUtil.from(future, scheduler);
    }

    @Override
    public Observable<PreparedStatement> prepare(String query) {
        ListenableFuture<PreparedStatement> future = session.prepareAsync(query);
        return RxUtil.from(future, Schedulers.computation());
    }

    @Override
    public Observable<PreparedStatement> prepare(String query, Scheduler scheduler) {
        ListenableFuture<PreparedStatement> future = session.prepareAsync(query);
        return RxUtil.from(future, scheduler);
    }

    @Override
    public Observable<PreparedStatement> prepare(RegularStatement statement) {
        ListenableFuture<PreparedStatement> future = session.prepareAsync(statement);
        return RxUtil.from(future, Schedulers.computation());
    }

    @Override
    public Observable<PreparedStatement> prepare(RegularStatement statement, Scheduler scheduler) {
        ListenableFuture<PreparedStatement> future = session.prepareAsync(statement);
        return RxUtil.from(future, scheduler);
    }

    @Override
    public void close() {
        session.close();
    }

    @Override
    public boolean isClosed() {
        return session.isClosed();
    }

    @Override
    public Cluster getCluster() {
        return session.getCluster();
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public Session.State getState() {
        return session.getState();
    }
}
