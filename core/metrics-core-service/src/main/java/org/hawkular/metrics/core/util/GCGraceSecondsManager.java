/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;

import com.datastax.driver.core.AggregateMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.FunctionMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.SchemaChangeListener;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.google.common.base.Stopwatch;

import rx.Completable;
import rx.Observable;
import rx.subjects.PublishSubject;

/**
 * <p>
 * A listener that manages gc_grace_seconds for each table. For a single node cluster gc_grace_seconds can safely be
 * set to zero for all tables. It can also be set to zero for a multi-node cluster when replication_factor = 1. We can
 * always keep gc_grace_seconds = 0 for the data_compressed table since we do append-only writes and use TTL for
 * deletes.
 * </p>
 * <p>
 * Checks are performed only on keyspace change events such as changes to replication_factor since we do not do any
 * schema changes like adding/altering tables at runtime after server initialization.
 * </p>
 *
 * @author jsanda
 */
public class GCGraceSecondsManager implements SchemaChangeListener {

    public static int DEFAULT_GC_GRACE_SECONDS = 604800;

    private static Logger logger = Logger.getLogger(GCGraceSecondsManager.class);

    private RxSession session;

    private PreparedStatement getGCGraceSeconds;

    private String keyspace;

    private ConfigurationService configurationService;

    private Optional<PublishSubject<Throwable>> updatesFinished;

    public GCGraceSecondsManager(RxSession session, String keyspace, ConfigurationService configurationService) {
        this.session = session;
        this.keyspace = keyspace;
        getGCGraceSeconds = session.getSession().prepare(
                "SELECT table_name, gc_grace_seconds FROM system_schema.tables WHERE keyspace_name = ?");
        this.configurationService = configurationService;
        session.getCluster().register(this);
        updatesFinished = Optional.empty();

    }

    /**
     * Asynchronously performs gc_grace_seconds updates if necessary. This method should <strong>not</strong> be called
     * until schema updates have finished.
     */
    public void maybeUpdateGCGraceSeconds() {
        logger.info("Checking tables in " + keyspace + " to see if gc_grace_seconds needs to be updated");
        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<String, String> replication = session.getCluster().getMetadata().getKeyspace(keyspace).getReplication();
        String replicationFactor = replication.get("replication_factor");
        Completable check;

        if (getClusterSize() == 1 || replicationFactor.equals("1")) {
            check = updateAllGCGraceSeconds(0);
        } else {
            // Need to call Completable.merge in order for subscriptions to happen correctly. See https://goo.gl/l15CRV
            check = Completable.merge(configurationService.load("org.hawkular.metrics", "gcGraceSeconds")
                    .switchIfEmpty(Observable.just(Integer.toString(DEFAULT_GC_GRACE_SECONDS)))
                    .map(property -> {
                        int gcGraceSeconds = Integer.parseInt(property);
                        return updateAllGCGraceSeconds(gcGraceSeconds);
                    }));
        }

        check.subscribe(
                () -> {
                    stopwatch.stop();
                    logger.info("Finished gc_grace_seconds updates in " + stopwatch.elapsed(TimeUnit.MILLISECONDS)
                            + " ms");
                    updatesFinished.ifPresent(subject -> subject.onNext(null));
                },
                t -> {
                    logger.warn("There was an error checking and updating gc_grace_seconds");
                    updatesFinished.ifPresent(subject -> subject.onNext(t));
                }
        );
    }

    /**
     * Test hook
     */
    protected int getClusterSize() {
        return session.getCluster().getMetadata().getAllHosts().size();
    }

    void setUpdatesFinishedSubject(PublishSubject<Throwable> updatesFinishedSubject) {
        updatesFinished = Optional.of(updatesFinishedSubject);
    }

    private Completable updateAllGCGraceSeconds(int gcGraceSeconds) {
        return Completable.concat(getGCGraceSeconds().map(metaData -> {
            if (metaData.tableName.equals("data_compressed")) {
                if (metaData.gcGraceSeconds == 0) {
                    return Completable.complete();
                }
                logger.info("gc_grace_seconds for " + metaData.tableName + " is set to " + metaData.gcGraceSeconds +
                        ". Resetting back to zero.");
                return updateGCGraceSeconds(metaData.tableName, 0);
            } else {
                if (metaData.gcGraceSeconds != gcGraceSeconds) {
                    logger.info("gc_grace_seconds for " + metaData.tableName + " is set to " + metaData.gcGraceSeconds
                            + ". Resetting to " + gcGraceSeconds);
                    return updateGCGraceSeconds(metaData.tableName, gcGraceSeconds);
                }
                return Completable.complete();
            }
        }));
    }

    private Observable<TableMetaData> getGCGraceSeconds() {
        return session.executeAndFetch(getGCGraceSeconds.bind(keyspace))
                .map(row -> new TableMetaData(row.getString(0), row.getInt(1)));
    }

    private Completable updateGCGraceSeconds(String table, int seconds) {
        return session.execute("ALTER TABLE " + keyspace + "." + table + " WITH gc_grace_seconds = " + seconds)
                .toCompletable();
    }

    @Override
    public void onRegister(Cluster cluster) {

    }

    @Override
    public void onUnregister(Cluster cluster) {

    }

    @Override
    public void onKeyspaceAdded(KeyspaceMetadata keyspace) {
    }

    @Override
    public void onKeyspaceRemoved(KeyspaceMetadata keyspace) {
    }

    @Override
    public void onKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous) {
        if (!current.getName().equals(keyspace)) {
            return;
        }
        String oldReplicationFactor = previous.getReplication().get("replication_factor");
        String newReplicationFactor = current.getReplication().get("replication_factor");
        if (!oldReplicationFactor.equals(newReplicationFactor)) {
            logger.info("replication_factor of " + keyspace + " has changed from " + oldReplicationFactor +
                    " to " + newReplicationFactor);
            maybeUpdateGCGraceSeconds();
        }
    }

    @Override
    public void onTableAdded(TableMetadata table) {
    }

    @Override
    public void onTableRemoved(TableMetadata table) {
    }

    @Override
    public void onTableChanged(TableMetadata current, TableMetadata previous) {
    }

    @Override
    public void onUserTypeAdded(UserType type) {

    }

    @Override
    public void onUserTypeRemoved(UserType type) {

    }

    @Override
    public void onUserTypeChanged(UserType current, UserType previous) {

    }

    @Override
    public void onFunctionAdded(FunctionMetadata function) {

    }

    @Override
    public void onFunctionRemoved(FunctionMetadata function) {

    }

    @Override
    public void onFunctionChanged(FunctionMetadata current, FunctionMetadata previous) {

    }

    @Override
    public void onAggregateAdded(AggregateMetadata aggregate) {

    }

    @Override
    public void onAggregateRemoved(AggregateMetadata aggregate) {

    }

    @Override
    public void onAggregateChanged(AggregateMetadata current, AggregateMetadata previous) {

    }

    @Override
    public void onMaterializedViewAdded(MaterializedViewMetadata view) {

    }

    @Override
    public void onMaterializedViewRemoved(MaterializedViewMetadata view) {

    }

    @Override
    public void onMaterializedViewChanged(MaterializedViewMetadata current, MaterializedViewMetadata previous) {

    }

    private static class TableMetaData {
        private final String tableName;
        private final int gcGraceSeconds;

        public TableMetaData(String tableName, int gcGraceSeconds) {
            this.tableName = tableName;
            this.gcGraceSeconds = gcGraceSeconds;
        }
    }
}
