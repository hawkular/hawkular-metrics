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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openshift.internal.restclient.model.Pod;

/**
 * @author rvargasp
 */
public class Truncator {
    private static final Logger log = LoggerFactory.getLogger(Truncator.class);

    private static final String KUBERNETES_MASTER_URL_SYSPROP = "KUBERNETES_MASTER_URL";
    private static final String KUBERNETES_MASTER_URL_DEFAULT = "https://kubernetes.default.svc.cluster.local";
    private static final String TOKEN_FILE_LOCATION = "/var/run/secrets/kubernetes.io/serviceaccount/token";
    private static final String CASSANDRA_POD_PREFIX_DEFAULT = "hawkular-cassandra";
    private static final String METRICS_RC_NAME_DEFAULT = "hawkular-metrics";
    private static final String HEAPSTER_RC_NAME_DEFAULT = "heapster";
    private static final int PARTITION_THRESHOLD_DEFAULT = 104900000;

    private boolean useSSL;
    private boolean forceTruncation;

    private String KUBERNETES_MASTER_URL;
    private String cassandraPodPrefix;
    private String namespace;
    private String metricsRC;
    private String heapsterRC;
    private String keyspace;

    private int partitionThreshold;
    private int cassandraConnectionMaxRetries;

    private int truncatorMaxRetries;
    private long truncatorMaxDelay;
    private long cassandraConnectionMaxDelay;

    private OpenshiftClientWrapper openshiftClient;

    private int metricsReplicas, heapsterReplicas;

    public Truncator() {
        KUBERNETES_MASTER_URL = System.getProperty(KUBERNETES_MASTER_URL_SYSPROP, KUBERNETES_MASTER_URL_DEFAULT);
        metricsRC = System.getProperty("hawkular.metrics.rc-name", METRICS_RC_NAME_DEFAULT);
        heapsterRC = System.getProperty("hawkular.metrics.heapster.rc-name", HEAPSTER_RC_NAME_DEFAULT);
        cassandraPodPrefix = System.getProperty("hawkular.metrics.cassandra.pod-prefix", CASSANDRA_POD_PREFIX_DEFAULT);
        partitionThreshold = Integer.getInteger("hawkular.metrics.partition-threshold", PARTITION_THRESHOLD_DEFAULT);
        useSSL = Boolean.getBoolean("hawkular.metrics.cassandra.use-ssl");
        forceTruncation = Boolean.getBoolean("hawkular.metrics.force-truncation");
        keyspace = System.getProperty("hawkular.metrics.cassandra.keyspace", "hawkular_metrics");
        namespace = System.getProperty("hawkular.metrics.namespace", "openshift-infra");
        cassandraConnectionMaxDelay = Long.getLong("hawkular.metrics.cassandra.connection.max-delay", 30);
        cassandraConnectionMaxRetries = Integer.getInteger("hawkular.metrics.cassandra.connection.max-retries", 5);
        truncatorMaxRetries = Integer.getInteger("hawkular.metrics.truncator.max-retries", 5);
        truncatorMaxDelay = Long.getLong("hawkular.metrics.truncator.max-delay", 5*1000);
        metricsReplicas = heapsterReplicas = 0;
    }

    private String getToken() throws IOException {
        return new String(Files.readAllBytes(Paths.get(TOKEN_FILE_LOCATION)));
    }

    public void restoreControllers() {
        if (this.heapsterReplicas != 0) {
            openshiftClient.scaleReplicaitonController(this.heapsterRC, this.heapsterReplicas);
        }

        if (this.metricsReplicas != 0) {
            openshiftClient.scaleReplicaitonController(this.metricsRC, this.metricsReplicas);
        }
    }

    public void truncate() throws IOException, InterruptedException, RuntimeException {
        String token = this.getToken();
        openshiftClient = new OpenshiftClientWrapper(this.namespace, this.KUBERNETES_MASTER_URL, token);

        Pod[] cassandraPods = openshiftClient.getPodsWithPrefix(this.cassandraPodPrefix);
        ArrayList<String> cassandraHosts = new ArrayList<>();
        Arrays.stream(cassandraPods).forEach((pod) -> {
            cassandraHosts.add(pod.getIP());
        });
        int replicas;

        if (cassandraPods.length > 0) {

            if (openshiftClient.hasLargePartition(this.partitionThreshold, this.keyspace, this.cassandraPodPrefix)  || this.forceTruncation) {
                IndexTruncator truncator = new IndexTruncator(cassandraHosts, this.keyspace, this.useSSL);
                truncator.setCassandraConnectionMaxDelay(cassandraConnectionMaxDelay);
                truncator.setCassandraConnectionMaxRetries(cassandraConnectionMaxRetries);
                // Get current replicas
                replicas = openshiftClient.getReplicationControllerScale(this.metricsRC);
                if (replicas != 0) {
                    metricsReplicas = replicas;
                }

                replicas = openshiftClient.getReplicationControllerScale(this.heapsterRC);
                if (replicas != 0) {
                    heapsterReplicas = replicas;
                }

                // Scale to zero
                openshiftClient.scaleReplicaitonController(this.heapsterRC, 0);
                openshiftClient.scaleReplicaitonController(this.metricsRC, 0);

                    // Truncate
                truncator.execute();
            } else {
                log.info("Truncation was not necessary, skipping.");
            }
        } else {
            throw new RuntimeException("No cassandra pods available, Is Hawkular Metrics running?");
        }
    }

    private void logTruncatorProperties() {
        log.info("Configured truncator properties:\n" +
                "\thawkular.metrics.rc-name = " + metricsRC + "\n" +
                "\thawkular.metrics.heapster.rc-name = " + heapsterRC + "\n" +
                "\thawkular.metrics.cassandra.pod-prefix = " + cassandraPodPrefix + "\n" +
                "\thawkular.metrics.partition-threshold (bytes) = " + partitionThreshold + "\n" +
                "\thawkular.metrics.cassandra.use-ssl = " + useSSL + "\n" +
                "\thawkular.metrics.force-truncation = " + forceTruncation + "\n" +
                "\thawkular.metrics.cassandra.keyspace = " + keyspace + "\n" +
                "\thawkular.metrics.cassandra.connection.max-delay = " + cassandraConnectionMaxDelay + "\n" +
                "\thawkular.metrics.cassandra.connection.max-retries = " + cassandraConnectionMaxRetries + "\n" +
                "\thawkular.metrics.truncator.max-retries = " + truncatorMaxRetries +
                "\thawkular.metrics.truncator.max-delay = " + truncatorMaxDelay);
    }


    public void run() {
        this.logTruncatorProperties();
        int retryCounter = 0;
        while (retryCounter < truncatorMaxRetries) {
            try {
                this.truncate();
                break;
            } catch (Exception e) {
                retryCounter++;
                log.error("truncation failed on retry " + retryCounter + " of " + truncatorMaxRetries +
                        ", retry again in " + truncatorMaxDelay + "seconds.");
                if (retryCounter >= truncatorMaxRetries) {
                    e.printStackTrace();
                    break;
                }
                try {
                    Thread.sleep(truncatorMaxDelay);
                } catch (InterruptedException ie) {
                    break;
                }
            }
        }
        // failed after n attempts, no matter what happens we need to restore controller replicas.
        this.restoreControllers();
    }

    public static void main(String[] args) {
        new Truncator().run();
    }


}
