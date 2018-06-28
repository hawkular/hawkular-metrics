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

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.log.CoreLogger;
import org.hawkular.metrics.core.service.log.CoreLogging;
import org.hawkular.metrics.datetime.DateTimeService;
import org.joda.time.DateTime;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author Michael Burman
 */
public class TestDataAccessFactory {

    private static final CoreLogger log = CoreLogging.getCoreLogger(TestDataAccessFactory.class);

    public static DataAccess newInstance(Session session) {
        return newInstance(session, DateTimeService.now.get());
    }

    public static DataAccess newInstance(Session session, DateTime now) {
        return newInstance(session, now, true);
    }

    public static DataAccess newInstance(Session session, DateTime now, boolean dropTempTables) {
        session.execute(String.format("USE %s", BaseITest.getKeyspace()));
        final CountDownLatch latch = new CountDownLatch(3);
        final CountDownLatch fallBackTable = new CountDownLatch(0);
        DataAccessImpl dataAccess = new DataAccessImpl(session) {
            @Override
            void prepareTempStatements(String tableName, Long mapKey) {
                super.prepareTempStatements(tableName, mapKey);
                if(DataAccessImpl.OUT_OF_ORDER_TABLE_NAME.equals(tableName)) {
                    log.infof("data_0 statements being prepared");
                    fallBackTable.countDown();
                }
                if (latch.getCount() > 0) {
                    latch.countDown();
                }
            }

            @Override public Observable<ResultSet> dropTempTable(long timestamp) {
                if (dropTempTables) {
                    return super.dropTempTable(timestamp);
                }
                return Observable.empty();
            }
        };
        dataAccess.createTempTablesIfNotExists(tableListForTesting(now))
                .subscribeOn(Schedulers.io())
                .toBlocking().subscribe();
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertTrue(fallBackTable.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return dataAccess;
    }

    /**
     * Create few temporary tables for tests
     */
    static Set<Long> tableListForTesting(DateTime now) {
        Set<Long> tempTables = new HashSet<>(3);
        tempTables.add(now.getMillis());
        tempTables.add(now.minusHours(2).getMillis());
        tempTables.add(now.plusHours(2).getMillis());
        return tempTables;
    }

}
