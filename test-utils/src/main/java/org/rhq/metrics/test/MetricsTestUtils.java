/*
 * Copyright 2014 Red Hat, Inc.
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

package org.rhq.metrics.test;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.rules.ExternalResource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

public final class MetricsTestUtils extends ExternalResource {
    private static final long FUTURE_TIMEOUT = 3;
    private static Session session;

    private MetricsTestUtils() {
        super();
    }

    /**
     * Returns a {@link Session} singleton opened lazily using system properties {@code "nodes"} and {@code "keyspace"}.
     * <p>
     * The returned session must not be closed by callers.
     * <p>
     * As this method is for testing purposes only, no attempts are done to close the returned session at the end of
     * the run of a test or test suite.
     *
     * @return a {@link Session}
     */
    public static synchronized Session getSession() {
        if (session == null) {
            String nodeAddresses = System.getProperty("nodes", "127.0.0.1");
            Cluster cluster = new Cluster.Builder().addContactPoints(nodeAddresses.split(","))
            // Due to JAVA-500 and JAVA-509 we need to explicitly set the protocol to V3.
            // These bugs are fixed upstream and will be in version 2.1.3 of the driver.
                    .withProtocolVersion(ProtocolVersion.V3).build();
            String keyspace = System.getProperty("keyspace", "rhqtest");
            session = cluster.connect(keyspace);
        }
        return session;
    }

    public static <V> V getUninterruptibly(ListenableFuture<V> future) throws ExecutionException, TimeoutException {
        return Uninterruptibles.getUninterruptibly(future, FUTURE_TIMEOUT, TimeUnit.SECONDS);
    }

}
