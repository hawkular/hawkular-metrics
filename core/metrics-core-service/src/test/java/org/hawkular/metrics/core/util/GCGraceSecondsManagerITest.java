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
package org.hawkular.metrics.core.util;

import static org.hawkular.metrics.core.util.GCGraceSecondsManager.DEFAULT_GC_GRACE_SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.core.service.BaseITest;
import org.hawkular.metrics.schema.SchemaService;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.ResultSet;

import rx.Subscription;
import rx.subjects.PublishSubject;

/**
 * @author jsanda
 */
public class GCGraceSecondsManagerITest extends BaseITest {

    private static class TestGCGraceSecondsManager extends GCGraceSecondsManager {

        private Integer clusterSize;

        private PublishSubject<Throwable> updatesFinished;

        public TestGCGraceSecondsManager(RxSession session, String keyspace,
                ConfigurationService configurationService) {
            super(session, keyspace, configurationService);
            updatesFinished = PublishSubject.create();
            setUpdatesFinishedSubject(updatesFinished);
        }

        @Override
        protected int getClusterSize() {
            if (clusterSize == null) {
                return super.getClusterSize();
            }
            return clusterSize;
        }

        public void setClusterSize(int clusterSize) {
            this.clusterSize = clusterSize;
        }

        @Override
        public void maybeUpdateGCGraceSeconds() {
            Subscription subscription = null;
            try {
                CountDownLatch latch = new CountDownLatch(1);
                AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
                subscription = updatesFinished.subscribe(t -> {
                    exceptionRef.set(t);
                    latch.countDown();
                });
                super.maybeUpdateGCGraceSeconds();
                latch.await(10, TimeUnit.SECONDS);
                if (exceptionRef.get() != null) {
                    fail("Updates failed", exceptionRef.get());
                }
            } catch (InterruptedException e) {
                fail("Timed out waiting for updates to complete", e);
            } finally {
                if (subscription != null) {
                    subscription.unsubscribe();
                }
            }
        }
    }

    private ConfigurationService configurationService;

    private String keyspace = "gc_grace_test";

    private TestGCGraceSecondsManager manager;

    @BeforeClass
    public void initClass() {
        configurationService = new ConfigurationService();
        configurationService.init(rxSession);

        SchemaService schemaService = new SchemaService();
        schemaService.run(session, keyspace, Boolean.valueOf(System.getProperty("resetdb", "true")));

        manager = new TestGCGraceSecondsManager(rxSession, keyspace, configurationService);
        session.getCluster().register(manager);
    }

    @Test
    public void singleNodeCluster() throws Exception {
        manager.maybeUpdateGCGraceSeconds();

        ResultSet resultSet = session.execute(
                "select table_name, gc_grace_seconds from system_schema.tables where keyspace_name = '" + keyspace +
                        "'");
        resultSet.forEach(row -> assertEquals(row.getInt(1), 0, "gc_grace_seconds for " + row.getString(0) +
                " is wrong"));
    }

    @Test(dependsOnMethods = "singleNodeCluster")
    public void multiNodeClusterWithRF2() throws Exception {
        manager.setClusterSize(3);

        session.execute("alter keyspace " + keyspace + " with replication = {'class': 'SimpleStrategy', " +
                "'replication_factor': 2}");

        manager.maybeUpdateGCGraceSeconds();

        ResultSet resultSet = session.execute(
                "select table_name, gc_grace_seconds from system_schema.tables where keyspace_name = '" + keyspace +
                        "'");
        resultSet.all().forEach(row -> {
            if (row.getString(0).equals("data_compressed")) {
                assertEquals(row.getInt(1), 0, "gc_grace_seconds for " + row.getString(0) + " is wrong");
            } else {
                assertEquals(row.getInt(1), DEFAULT_GC_GRACE_SECONDS,
                        "gc_grace_seconds for " + row.getString(0) + " is wrong");
            }
        });
    }

}
