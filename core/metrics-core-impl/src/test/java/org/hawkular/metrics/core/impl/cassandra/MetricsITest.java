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
package org.hawkular.metrics.core.impl.cassandra;

import static org.joda.time.DateTime.now;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.joda.time.DateTime;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * @author John Sanda
 */
public class MetricsITest {

    private static final long FUTURE_TIMEOUT = 3;

    protected Session session;

    private PreparedStatement truncateMetrics;

    private PreparedStatement truncateCounters;

    public void initSession() {
        String nodeAddresses = System.getProperty("nodes", "127.0.0.1");
        Cluster cluster = new Cluster.Builder()
            .addContactPoints(nodeAddresses.split(","))
            // Due to JAVA-500 and JAVA-509 we need to explicitly set the protocol to V3.
            // These bugs are fixed upstream and will be in version 2.1.3 of the driver.
            .withProtocolVersion(ProtocolVersion.V3)
            .build();
        session = cluster.connect(getKeyspace());

//        truncateMetrics = session.prepare("TRUNCATE metrics");
//        truncateCounters = session.prepare("TRUNCATE counters");
    }

    protected void resetDB() {
        session.execute(truncateMetrics.bind());
        session.execute(truncateCounters.bind());
    }

    protected String getKeyspace() {
        return System.getProperty("keyspace", "hawkular");
    }

    protected DateTime hour0() {
        DateTime rightNow = now();
        return rightNow.hourOfDay().roundFloorCopy().minusHours(
            rightNow.hourOfDay().roundFloorCopy().hourOfDay().get());
    }

    protected DateTime hour(int hourOfDay) {
        return hour0().plusHours(hourOfDay);
    }

    protected <V> V getUninterruptibly(ListenableFuture<V> future) throws ExecutionException, TimeoutException {
        return Uninterruptibles.getUninterruptibly(future, FUTURE_TIMEOUT, TimeUnit.SECONDS);
    }

}
