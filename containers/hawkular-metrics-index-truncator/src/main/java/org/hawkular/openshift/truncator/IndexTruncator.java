/*
 * Copyright 2014-2019 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.openshift.truncator;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * @author rvargasp
 */
public class IndexTruncator {
    private static String TRUNCATE_TAGS_IDX_QUERY = "TRUNCATE TABLE metrics_tags_idx";
    private static String TRUNCATE_METRICS_IDX_QUERY = "TRUNCATE TABLE metrics_idx";
    private static final Logger log = LoggerFactory.getLogger(IndexTruncator.class);


    private ArrayList<String> cassandraNodes;
    private boolean useSSL;
    private String keyspace;
    private long cassandraConnectionMaxDelay = 30*1000;
    private int cassandraConnectionMaxRetries = 5;
    private Session session;
    private Cluster cluster;

    IndexTruncator(ArrayList<String> cassandraNodes, String keyspace, boolean useSSL) {
        this.cassandraNodes = cassandraNodes;
        this.useSSL = useSSL;
        this.keyspace = keyspace;
    }

    public void setCassandraConnectionMaxDelay(long delay) {
        this.cassandraConnectionMaxDelay = delay*1000;
    }

    public void setCassandraConnectionMaxRetries(int retries) {
        this.cassandraConnectionMaxRetries = retries;
    }

    public void execute()  throws RuntimeException, InterruptedException {
        try {
            initSession();
            session.execute(TRUNCATE_TAGS_IDX_QUERY);
            session.execute(TRUNCATE_METRICS_IDX_QUERY);
        } finally {
            this.shutdown();
        }
    }

    private void initSession() throws RuntimeException, InterruptedException {
        int retryCounter = cassandraConnectionMaxRetries;
        while (retryCounter-- != 0) {
            try {
                createSession();
                return;
            } catch (NoHostAvailableException e) {
                log.error(e.getMessage());
                Thread.sleep(cassandraConnectionMaxDelay);
            }
        }
        throw new RuntimeException("Cassandra connection retry limit exceeded.");
    }
    private void shutdown() {
        if (session != null) {
            session.close();
            session = null;
        }
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }


    private void createSession() {
        Cluster.Builder clusterBuilder = new Cluster.Builder();
        clusterBuilder.addContactPoints(cassandraNodes.toArray(new String[]{}));
        if (useSSL) {
            SSLOptions sslOptions;
            try {
                String[] defaultCipherSuites = {"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"};
                sslOptions = JdkSSLOptions.builder().withSSLContext(SSLContext.getDefault())
                        .withCipherSuites(defaultCipherSuites).build();
                clusterBuilder.withSSL(sslOptions);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SSL support is required but is not available in the JVM.", e);
            }
        }

        clusterBuilder.withoutJMXReporting();

        this.cluster = clusterBuilder.build();
        this.cluster.init();
        session = null;
        try {
            session = cluster.connect(this.keyspace);
        } finally {
            if (session == null) {
                cluster.close();
            }
        }
    }
}
