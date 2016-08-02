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

import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import rx.Observable;
import rx.Scheduler;

/**
 * @author jsanda
 */
class LockManager {

    private static Logger logger = Logger.getLogger(LockManager.class);

    private RxSession session;

    private PreparedStatement acquireLock;

    private PreparedStatement releaseLock;

    public LockManager(RxSession session) {
        this.session = session;
        acquireLock = session.getSession().prepare(
                "UPDATE locks USING TTL ? SET value = ? WHERE name = ? IF value IN (NULL, ?)");
        releaseLock = session.getSession().prepare(
                "UPDATE locks SET value = NULL WHERE name = ? IF value = ?");
    }

    public Observable<Boolean> acquireLock(String name, String value, int timeout) {
        return session.execute(acquireLock.bind(timeout, value, name, value)).map(ResultSet::wasApplied);
    }

    public Observable<Boolean> acquireLock(String name, String value, int timeout, Scheduler scheduler) {
        return session.execute(acquireLock.bind(timeout, value, name), scheduler).map(ResultSet::wasApplied);
    }

    public Observable<Boolean> releaseLock(String name, String value) {
        return session.execute(releaseLock.bind(name, value)).map(ResultSet::wasApplied);
    }

}
