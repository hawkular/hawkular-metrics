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
package org.hawkular.metrics.schema;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * @author jsanda
 */
public class Installer {

    private static final Logger logger = LoggerFactory.getLogger(Installer.class);

    private static final String IMPL_VERSION = "Implementation-Version";

    private static final String GIT_SHA = "Built-From-Git-SHA1";

    private List<String> cassandraNodes;

    private boolean useSSL;

    private int cqlPort;

    private String keyspace;

    private boolean resetdb;

    private int replicationFactor;

    // Currently the installer is configured via system properties, and none of its fields are exposed as properties.
    // If the need should arise, the fields can be exposed as properties with public getter/setter methods.
    public Installer() {
        cqlPort = Integer.getInteger("hawkular.metrics.cassandra.cql-port", 9042);
        useSSL = Boolean.getBoolean("hawkular.metrics.cassandra.use-ssl");
        String nodes = System.getProperty("hawkular.metrics.cassandra.nodes", "127.0.0.1");
        cassandraNodes = new ArrayList<>();
        Arrays.stream(nodes.split(",")).forEach(cassandraNodes::add);
        keyspace = System.getProperty("hawkular.metrics.cassandra.keyspace", "hawkular_metrics");
        resetdb = Boolean.getBoolean("hawkular.metrics.cassandra.resetdb");
        replicationFactor = Integer.getInteger("hawkular.metrics.cassandra.replication-factor", 1);
    }

    public void run() {
        logVersion();
        logInstallerProperties();

        Session session = null;
        try {
            session = initSession();

            SchemaService schemaService = new SchemaService();
            schemaService.run(session, keyspace, resetdb);

            session.close();
            session.getCluster().close();

            logger.info("Finished applying schema updates");
        } catch (InterruptedException e) {
            logger.warn("Aborting installation");
        } finally {
            if (session != null) {
                session.getCluster().close();
            }
        }
    }

    private void logVersion() {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("META-INF/MANIFEST.MF")) {
            Manifest manifest = new Manifest(stream);
            Attributes attributes = manifest.getMainAttributes();
            String version = attributes.getValue(IMPL_VERSION);
            String gitSHA = attributes.getValue(GIT_SHA);

            logger.info("Hawkular Metrics Schema Installer v{}+{}", version, gitSHA.substring(0, 10));
        } catch (IOException e) {
            logger.warn("Failed to log version info", e);
        }
    }

    private void logInstallerProperties() {
        logger.info("Configured installer properties:\n" +
                "\tcqlPort = " + cqlPort + "\n" +
                "\tuseSSL = " + useSSL + "\n" +
                "\tcassandraNodes = " + cassandraNodes + "\n" +
                "\tkeyspace = " + keyspace + "\n" +
                "\tresetdb = " + resetdb + "\n" +
                "\treplicationFactor = " + replicationFactor);
    }

    private Session initSession() throws InterruptedException {
        long retry = 5000;
        while (true) {
            try {
                return createSession();
            } catch (NoHostAvailableException e) {
                logger.info("Cassandra may not be up yet. Retrying in {} ms", retry);
                Thread.sleep(retry);
            }
        }
    }

    private Session createSession() {
        Cluster.Builder clusterBuilder = new Cluster.Builder();
        clusterBuilder.addContactPoints(cassandraNodes.toArray(new String[] {}));
        if (useSSL) {
            SSLOptions sslOptions = null;
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

        Cluster cluster = clusterBuilder.build();
        cluster.init();
        Session createdSession = null;
        try {
            createdSession = cluster.connect("system");
            return createdSession;
        } finally {
            if (createdSession == null) {
                cluster.close();
            }
        }
    }

    public static void main(String[] args) {
        new Installer().run();
    }

}
