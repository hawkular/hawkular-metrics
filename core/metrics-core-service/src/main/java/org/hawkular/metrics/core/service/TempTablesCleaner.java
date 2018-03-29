/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.service;

import java.util.concurrent.TimeUnit;

import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;
import org.joda.time.Days;

import com.datastax.driver.core.PreparedStatement;

import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author jsanda
 */
public class TempTablesCleaner {

    private static Logger logger = Logger.getLogger(TempTablesCleaner.class);

    private RxSession session;

    private PreparedStatement findTables;

    private long ttl;

    private DataAccessImpl dataAccess;

    private volatile boolean finished;

    private static final String DROP_TABLE_CQL = "DROP TABLE IF EXISTS %s";

    public TempTablesCleaner(RxSession session, DataAccessImpl dataAccess, String keyspace, int ttl) {
        this.session = session;
        this.dataAccess = dataAccess;
        this.ttl = Days.days(ttl).toStandardDuration().getMillis();

        findTables = session.getSession().prepare(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '" + keyspace + "'");
    }

    public void run() {
        logger.info("Checking for expired temp tables");
        Observable.interval(1, TimeUnit.DAYS, Schedulers.io())
                .takeUntil(i -> finished)
                .flatMap(i -> session.execute(findTables.bind()))
                .compose(applyRetryPolicy())
                .flatMap(Observable::from)
                .filter(row -> row.getString(0).startsWith(DataAccessImpl.TEMP_TABLE_NAME_PROTOTYPE))
                .map(row -> row.getString(0))
                .filter(this::isTableExpired)
                .flatMap(this::dropTable)
                .filter(table -> !table.isEmpty())
                .subscribe(
                        table -> logger.infof("Dropped table %s", table),
                        t -> logger.warn("Cleaning temp tables failed", t),
                        () -> logger.infof("Finished cleaning expired temp tables")
                );

    }

    public void shutdown() {
        finished = true;
    }

    private <T> Observable.Transformer<T, T> applyRetryPolicy() {
        return tObservable -> tObservable
                .retryWhen(observable -> {
                    Integer maxRetries = Integer.getInteger("hawkular.metrics.temp-table-cleaner.max-retries", 10);
                    Integer maxDelay = Integer.getInteger("hawkular.metrics.temp-table-cleaner.max-delay", 300);
                    Observable<Integer> range = Observable.range(1, maxRetries);
                    Observable<Observable<?>> zipWith = observable.zipWith(range, (t, i) -> {
                        int delay = Math.min((int) Math.pow(2, i), maxDelay);
                        logger.debugf(t, "The findTables query failed. Attempting retry # %d seconds", delay);
                        return Observable.timer(delay, TimeUnit.SECONDS).onBackpressureDrop();
                    });

                    return Observable.merge(zipWith);
                });
    }

    private boolean isTableExpired(String table) {
        Long timestamp = dataAccess.tableToMapKey(table);
        return timestamp < (System.currentTimeMillis() - ttl);
    }

    private Observable<String> dropTable(String table) {
        return session.execute(String.format(DROP_TABLE_CQL, table))
                .map(resultSet -> table)
                .onErrorResumeNext(t -> {
                    // If there is an error, we do not retry because it is possible that the table has already been
                    // dropped. We will instead wait until findTables runs again and retry dropping the table then
                    // if dropping it did indeed fail for some reason.
                    logger.infof(t, "Failed to drop %s", table);
                    return Observable.just("");
                });
    }

}
