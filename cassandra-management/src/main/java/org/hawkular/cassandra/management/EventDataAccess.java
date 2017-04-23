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
package org.hawkular.cassandra.management;

import java.util.Date;

import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.joda.time.DateTime;

import com.datastax.driver.core.PreparedStatement;

import rx.Completable;
import rx.Observable;

/**
 * @author jsanda
 */
public class EventDataAccess {

    private RxSession session;

    private PreparedStatement insertEvent;

    private PreparedStatement findEvents;

    public EventDataAccess(RxSession session) {
        this.session = session;
        insertEvent = session.getSession().prepare(
                "INSERT INTO cassandra_mgmt_history (bucket, time, event, details) VALUES (?, ?, ?, ?)");
        findEvents = session.getSession().prepare(
                "SELECT time, event, details FROM cassandra_mgmt_history WHERE bucket = ?");
    }

    public Completable addEvent(Event event) {
        long bucket = DateTimeService.get24HourTimeSlice(new DateTime(event.getTimestamp())).getMillis();
        return session.execute(insertEvent.bind(new Date(bucket), new Date(event.getTimestamp()),
                event.getType().getCode(), event.getDetails())).toCompletable();
    }

    public Observable<Event> findEventsForBucket(long bucket) {
        return session.execute(findEvents.bind(new Date(bucket)))
                .flatMap(Observable::from)
                .map(row -> new Event(row.getTimestamp(0).getTime(), EventType.fromCode(row.getShort(1)),
                        row.getMap(2, String.class, String.class)));
    }

}
