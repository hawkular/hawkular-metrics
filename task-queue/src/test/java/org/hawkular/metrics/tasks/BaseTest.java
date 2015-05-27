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
package org.hawkular.metrics.tasks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.hawkular.metrics.schema.SchemaManager;
import org.hawkular.metrics.tasks.impl.Queries;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

/**
 * @author jsanda
 */
public class BaseTest {

    private static final long FUTURE_TIMEOUT = 3;

    protected static Session session;

    protected static RxSession rxSession;

    protected static DateTimeService dateTimeService;

    protected static Queries queries;

    @BeforeSuite
    public static void initSuite() throws Exception {
        Cluster cluster = Cluster.builder().addContactPoints("127.0.0.01").build();
        String keyspace = System.getProperty("keyspace", "hawkulartest");
        session = cluster.connect("system");
        rxSession = new RxSessionImpl(session);

        SchemaManager schemaManager = new SchemaManager(session);
        schemaManager.createSchema(keyspace);

        session.execute("USE " + keyspace);

        queries = new Queries(session);
        dateTimeService = new DateTimeService();
    }

    @BeforeMethod
    protected void resetDB() {
        session.execute("TRUNCATE task_queue");
        session.execute("TRUNCATE leases");
    }

    protected <V> V getUninterruptibly(ListenableFuture<V> future) throws ExecutionException, TimeoutException {
        return Uninterruptibles.getUninterruptibly(future, FUTURE_TIMEOUT, TimeUnit.SECONDS);
    }

}
