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
package org.hawkular.metrics.tasks.impl;

import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.hawkular.metrics.tasks.api.RepeatingTrigger;
import org.hawkular.metrics.tasks.api.Task2;
import org.hawkular.metrics.tasks.api.TaskScheduler;
import org.hawkular.rx.cassandra.driver.RxSession;
import rx.Observable;

/**
 * @author jsanda
 */
public class TaskSchedulerImpl implements TaskScheduler {

    private int numShards = Integer.parseInt(System.getProperty("hawkular.scheduler.shards", "10"));

    private HashFunction hashFunction = Hashing.murmur3_128();

    private RxSession session;

    private Queries queries;

    public TaskSchedulerImpl(RxSession session, Queries queries) {
        this.session = session;
        this.queries = queries;
    }

    @Override
    public Observable<Task2> createTask(String name, Map<String, String> parameters, RepeatingTrigger trigger) {
        UUID id = UUID.randomUUID();
        int shard = computeShard(id);
        UserType triggerType = getKeyspace().getUserType("trigger_def");
        UDTValue triggerUDT = triggerType.newValue();
        triggerUDT.setInt("type", 1);
        triggerUDT.setLong("interval", trigger.getInterval());

        return Observable.create(subscriber ->
            session.execute(queries.createTask2.bind(id, shard, name, parameters, triggerUDT)).subscribe(
                    resultSet -> subscriber.onNext(new Task2Impl(id, shard, name, parameters, trigger)),
                    t -> subscriber.onError(new RuntimeException("Failed to create task", t)),
                    subscriber::onCompleted
            )
        );
    }

    public Observable<Task2> findTask(UUID id) {
        return Observable.create(subscriber ->
            session.execute(queries.findTask.bind(id)).flatMap(Observable::from).subscribe(
                    row -> subscriber.onNext(new Task2Impl(id, row.getInt(0), row.getString(1),
                            row.getMap(2, String.class, String.class), getTrigger(row))),
                    t -> subscriber.onError(new RuntimeException("Failed to find task with id " + id, t)),
                    subscriber::onCompleted
            )
        );
    }

    private int computeShard(UUID uuid) {
        HashCode hashCode = hashFunction.hashBytes(uuid.toString().getBytes());
        return Hashing.consistentHash(hashCode, numShards);
    }

    private KeyspaceMetadata getKeyspace() {
        return session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
    }

    private RepeatingTrigger getTrigger(Row row) {
        UDTValue value = row.getUDTValue(3);
        return new RepeatingTrigger(value.getLong("interval"));
    }
}