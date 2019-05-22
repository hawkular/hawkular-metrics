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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openshift.internal.restclient.ResourceFactory;
import com.openshift.internal.restclient.model.Pod;
import com.openshift.internal.restclient.model.ReplicationController;
import com.openshift.restclient.ClientBuilder;
import com.openshift.restclient.IClient;
import com.openshift.restclient.ResourceKind;
import com.openshift.restclient.api.capabilities.IPodExec;
import com.openshift.restclient.api.capabilities.IScalable;
import com.openshift.restclient.apis.autoscaling.models.IScale;
import com.openshift.restclient.capability.CapabilityVisitor;
import com.openshift.restclient.capability.IStoppable;
import com.openshift.restclient.model.IReplicationController;



/**
 * @author rvargasp
 */
public class OpenshiftClientWrapper {
    private static final String NODETOOL_EXECUTABLE = "nodetool";
    private String namespace;
    private IClient client;
    private int execTimeOut = 60;
    private static final Logger log = LoggerFactory.getLogger(Truncator.class);

    OpenshiftClientWrapper(String namespace, String masterURL, String token) {
        this.namespace = namespace;
        IClient client = new ClientBuilder(masterURL).build();
        client.getAuthorizationContext().setToken(token);
        this.client = client;
    }

    public void scaleReplicaitonController(String name, int replicas) {
        IReplicationController replicationController = new ResourceFactory(client).create("v1", ResourceKind.REPLICATION_CONTROLLER);
        ((ReplicationController) replicationController).setName(name);
        ((ReplicationController) replicationController).setNamespace(this.namespace);
        replicationController.accept(new CapabilityVisitor<IScalable, IScale>() {
            @Override
            public IScale visit(IScalable capability) {
                return capability.scaleTo(replicas);
            }
        }, null);
    }

    public int getReplicationControllerScale(String name) {
        IReplicationController p = client.get(ResourceKind.REPLICATION_CONTROLLER, name, this.namespace);
        return p.getDesiredReplicaCount();
    }

    public Pod[] getPodsWithPrefix(String prefix) {
        ArrayList<Pod> pods = new ArrayList<>();
        Pattern prefixPattern = Pattern.compile("^" + prefix + ".*");
        List<Pod> list = client.list(ResourceKind.POD, this.namespace);
        for (Pod pod : list) {
            if (prefixPattern.matcher(pod.getName()).matches() && "Running".equals(pod.getStatus())) {
                pods.add(pod);
            }
        }
        return pods.toArray(new Pod[0]);
    }

    private int getMaxPartitionOfTable(String table, String keyspace, Pod cassandraPod) throws java.lang.InterruptedException {
        // We are using exec nodetool because we don't have access to JMX metrics to determine
        // the estimated size of partitions, we use estimated max of tablehistograms.
        String[] echoCommand = {NODETOOL_EXECUTABLE, "tablehistograms", keyspace, table};
        PartitionSizeListener partitionSizeListener = new PartitionSizeListener();
        cassandraPod.accept(new CapabilityVisitor<IPodExec, IStoppable>() {
            @Override
            public IStoppable visit(IPodExec capability) {
                return capability.start(partitionSizeListener, null, echoCommand);
            }
        }, null);
        partitionSizeListener.await(execTimeOut, TimeUnit.SECONDS);
        return partitionSizeListener.getPartitionSize();
    }

    public boolean hasLargePartition(int threshold, String keyspace, String cassandraPrefix) throws java.lang.InterruptedException {
        Pod[] pods = this.getPodsWithPrefix(cassandraPrefix);
        if (pods.length > 0) {
            Pod cassandra = pods[0];
            int maxMetricIndexSize = getMaxPartitionOfTable("metrics_idx", keyspace, cassandra);
            int maxTagsIndexSize = getMaxPartitionOfTable("metrics_tags_idx", keyspace, cassandra);
            int maxPartition = Math.max(maxMetricIndexSize, maxTagsIndexSize);
            return maxPartition > threshold;
        } else {
            log.error("No cassandra pods available, Is Hawkular Metrics running?");
        }
        return false;
    }

    private class PartitionSizeListener implements IPodExec.IPodExecOutputListener {
        public final CountDownLatch ExecutionDone = new CountDownLatch(1);
        public final AtomicInteger partitionSize = new AtomicInteger(0);

        @Override
        public void onOpen() {
            log.info("Init execution of nodetool");
        }

        @Override
        public void onStdOut(String message) {
            String[] parts = message.split("\\s+");
            if (parts.length >= 4 && parts[0].toLowerCase().equals("max")) {
                this.partitionSize.set(Integer.parseInt(parts[4]));
            }
        }

        @Override
        public void onStdErr(String message) {
            log.error(message);

        }

        @Override
        public void onExecErr(String message) {
            log.error(message);
        }

        @Override
        public void onClose(int code, String reason) {
            log.info(reason);
            ExecutionDone.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
            log.info(t.getMessage());
            ExecutionDone.countDown();
        }

        public void await(int timeOut, TimeUnit unit) throws InterruptedException {
            ExecutionDone.await(timeOut, unit);
        }

        public int getPartitionSize() {
            return this.partitionSize.get();
        }
    }

}
