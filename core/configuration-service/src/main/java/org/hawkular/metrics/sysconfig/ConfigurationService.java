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
package org.hawkular.metrics.sysconfig;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;

import org.hawkular.rx.cassandra.driver.RxSession;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;

import rx.Completable;
import rx.Observable;
import rx.Scheduler;

/**
 * @author jsanda
 */
public class ConfigurationService {

    private RxSession session;

    private PreparedStatement findConfigurationGroup;

    private PreparedStatement findConfigurationValue;

    private PreparedStatement updateConfigurationValue;

    private PreparedStatement deleteConfigurationValue;

    private PreparedStatement deleteConfiguration;

    // TODO make async
    // I could have just as easily passed the session as a constructor arg. I am doing it in the init method because
    // eventually I would like service initialization async.
    public void init(RxSession session) {
        this.session = session;
        findConfigurationGroup = session.getSession()
                .prepare("SELECT name, value FROM sys_config WHERE config_id = ?")
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        findConfigurationValue = session.getSession()
                .prepare("SELECT value FROM sys_config WHERE config_id = ? AND name= ?")
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        updateConfigurationValue = session.getSession().prepare(
                "INSERT INTO sys_config (config_id, name, value) VALUES (?, ?, ?)")
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        deleteConfigurationValue = session.getSession().prepare(
                "DELETE FROM sys_config WHERE config_id =? and name = ?")
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        deleteConfiguration = session.getSession().prepare(
                "DELETE FROM sys_config WHERE config_id = ?")
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }

    public Observable<Configuration> load(String id) {
        return session.executeAndFetch(findConfigurationGroup.bind(id))
                .toMap(row -> row.getString(0), row -> row.getString(1))
                .map(map -> new Configuration(id, map));
    }

    public Observable<String> load(String id, String name) {
        return session.executeAndFetch(findConfigurationValue.bind(id, name))
                .map(row -> row.getString(0));
    }

    public Observable<Configuration> load(String id, Scheduler scheduler) {
        return session.executeAndFetch(updateConfigurationValue.bind(id), scheduler)
                .toMap(row -> row.getString(0), row -> row.getString(1))
                .map(map -> new Configuration(id, map));
    }

    public Observable<Void> save(Configuration configuration) {
        return Observable.from(configuration.getProperties().entrySet())
                .map(entry -> updateConfigurationValue.bind(configuration.getId(), entry.getKey(), entry.getValue()))
                .collect(() -> new BatchStatement(UNLOGGED), BatchStatement::add)
                .flatMap(batch -> session.execute(batch).map(resultSet -> null));
    }

    public Observable<Void> save(String configId, String name, String value) {
        return session.execute(updateConfigurationValue.bind(configId, name, value))
                .map(resultSet -> null);
    }

    public Observable<Void> save(String configId, String name, String value, Scheduler scheduler) {
        return session.execute(updateConfigurationValue.bind(configId, name, value), scheduler).map(resultSet -> null);
    }

    public Completable delete(String configId, String name) {
        return session.execute(deleteConfigurationValue.bind(configId, name)).toCompletable();
    }

    public Completable delete(String configId) {
        return session.execute(deleteConfiguration.bind(configId)).toCompletable();
    }
}
