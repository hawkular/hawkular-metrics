package org.hawkular.schema

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.SimpleStatement
/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

def executeCQL(String cql, Integer readTimeoutMillis = null) {
  logger.debug("CQL: $cql")
  def statement = new SimpleStatement(cql)
  statement.consistencyLevel = ConsistencyLevel.LOCAL_QUORUM

  if (readTimeoutMillis) {
    statement.readTimeoutMillis = readTimeoutMillis
  }
  return session.execute(statement)
}

def createTable(String table, String cql) {
  if (tableDoesNotExist(keyspace, table)) {
    logger.info("Creating table $table")
    executeCQL(cql)
  }
}

def createType(String type, String cql) {
  if (typeDoesNotExist(keyspace, type)) {
    logger.info("Creating type $type")
    executeCQL(cql)
  }
}

if (reset) {
  executeCQL("DROP KEYSPACE IF EXISTS $keyspace", 20000)
}

def keyspaceCreate = "CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class': 'SimpleStrategy', " +
    "'replication_factor': $replicationFactor}"

if (keyspace.equals("hawkulartest") || keyspace.equals("hawkular_metrics_rest_tests")) {
  keyspaceCreate = "$keyspaceCreate AND DURABLE_WRITES = 'false'"
}

executeCQL(keyspaceCreate)
executeCQL("USE $keyspace")

// The retentions map entries are {metric type, retention} key/value paris where
// retention is specified in days
createTable('tenants', """
CREATE TABLE tenants (
    id text PRIMARY KEY,
    retentions map<text, int>
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
""")

createType('aggregation_template', """
CREATE TYPE aggregation_template (
    type int,
    src text,
    interval text,
    fns set<text>
)
""")

createTable('tenants_by_time', """
CREATE TABLE tenants_by_time (
  bucket timestamp,
    tenant text,
    PRIMARY KEY (bucket, tenant)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
""")

createType('aggregate_data', """
CREATE TYPE aggregate_data (
    type text,
    value double,
    time timeuuid,
    src_metric text,
    src_metric_interval text
)
""")

createTable('data', """
CREATE TABLE data (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timeuuid,
    data_retention int static,
    n_value double,
    availability blob,
    l_value bigint,
    aggregates set<frozen <aggregate_data>>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
""")

createTable('metrics_tags_idx', """
CREATE TABLE metrics_tags_idx (
    tenant_id text,
    tname text,
    tvalue text,
    type tinyint,
    metric text,
    PRIMARY KEY ((tenant_id, tname), tvalue, type, metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
""")

createTable('metrics_idx', """
CREATE TABLE metrics_idx (
     tenant_id text,
    type tinyint,
    metric text,
    tags map<text, text>,
    data_retention int,
    PRIMARY KEY ((tenant_id, type), metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
""")

createTable('retentions_idx', """
CREATE TABLE retentions_idx (
    tenant_id text,
    type tinyint,
    metric text,
    retention int,
    PRIMARY KEY ((tenant_id, type), metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
""")

createType('trigger_def', """
CREATE TYPE trigger_def (
    type int,
    trigger_time bigint,
    delay bigint,
    interval bigint,
    repeat_count int,
    execution_count int
)
""")

createTable('tasks', """
CREATE TABLE tasks (
    id uuid,
    group_key text,
    name text,
    exec_order int,
    params map<text, text>,
    trigger frozen <trigger_def>,
    PRIMARY KEY (id)
)
""")

createTable('leases', """
CREATE TABLE leases (
    time_slice timestamp,
    shard int,
    owner text,
    finished boolean,
    PRIMARY KEY (time_slice, shard)
)
""")