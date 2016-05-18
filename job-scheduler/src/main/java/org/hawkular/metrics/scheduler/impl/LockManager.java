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
package org.hawkular.metrics.scheduler.impl;

import static java.util.Collections.singletonList;

import org.hawkular.rx.cassandra.driver.RxSession;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import rx.Observable;

/**
 * @author jsanda
 */
class LockManager {

    private RxSession session;

    private PreparedStatement lockPermits;

    private PreparedStatement unlockPermits;

    private PreparedStatement acquireLock;

    private PreparedStatement releaseLock;

    private PreparedStatement acquirePermit;

    private PreparedStatement releasePermit;


    public LockManager(RxSession session) {
        this.session = session;
        lockPermits = session.getSession().prepare(
                "UPDATE locks USING TTL ? SET value = ? WHERE name = ? IF permits = NULL");
        unlockPermits = session.getSession().prepare(
                "UPDATE locks SET value = NULL WHERE name = ? IF value = ?");
        acquireLock = session.getSession().prepare(
                "UPDATE locks USING TTL ? SET value = ? WHERE name = ? IF value = NULL");
        releaseLock = session.getSession().prepare(
                "UPDATE locks SET value = NULL WHERE name = ? IF value = ?");
        acquirePermit = session.getSession().prepare(
                "UPDATE locks USING TTL ? SET permits = permits + ? WHERE name = ? IF value = NULL");
        releasePermit = session.getSession().prepare(
                "UPDATE locks SET permits = permits - ? WHERE name = ? IF value = NULL");
    }

    public Observable<Boolean> acquireLock(String name, String value, int timeout) {
        return session.execute(acquireLock.bind(timeout, value, name)).map(ResultSet::wasApplied);
    }

    public Observable<Boolean> releaseLock(String name, String value) {
        return session.execute(releaseLock.bind(name, value)).map(ResultSet::wasApplied);
    }

    public Observable<Boolean> acquirePermit(String name, String value, int timeout) {
        return session.execute(acquirePermit.bind(timeout, singletonList(value), name)).map(ResultSet::wasApplied);
    }

    public Observable<Boolean> releasePermit(String name, String value) {
        return session.execute(releasePermit.bind(singletonList(value), name)).map(ResultSet::wasApplied);
    }

    public Observable<Boolean> lockPermits(String name, String value, int timeout) {
        return session.execute(lockPermits.bind(timeout, value, name)).map(ResultSet::wasApplied);
    }

    public Observable<Boolean> unlockPermits(String name, String value) {
        return session.execute(unlockPermits.bind(name, value)).map(ResultSet::wasApplied);
    }

}
