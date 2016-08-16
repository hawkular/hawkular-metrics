package org.hawkular.schema

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.SimpleStatement

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

/**
 * Because we have releases of Hawkular Metrics prior to using cassalog, we need some special logic to initially
 * get things set up. That is the purpose of this script. If the target keyspace does not exist, then we have a new
 * installation and the script installs the schema as it exists prior to cassalog integration. If the target keyspace
 * exists, we check to see if it is already versions, i.e., managed by cassalog. If so, then we assume that there
 * is nothing left for this script to do. If the keyspace is not versioned, then we check that the schema matches
 * what we expect it to be to ensure we are in a known, consistent state before we start managing it with cassalog.
 */

def executeCQL(String cql, Integer readTimeoutMillis = null) {
  def statement = new SimpleStatement(cql)
  statement.consistencyLevel = ConsistencyLevel.LOCAL_QUORUM

  if (readTimeoutMillis) {
    statement.readTimeoutMillis = readTimeoutMillis
  }

  return session.execute(statement)
}

// We check the reset flag here because if we are resetting the database as would be the case in a dev/test environment,
// we don't need to worry about the state of the schema since we are dropping it.
if (!reset && keyspaceExists(keyspace)) {
  if (isSchemaVersioned(keyspace)) {
    // nothing to do
  } else {
    // If the schema exists and is not versioned, then we want to check that it matches what we expect it to be prior
    // to being managed with cassalog. We perform this check simply by checking the tables and UDTs in the keyspace.

    def expectedTables = [
        'tenants', 'tenants_by_time', 'data', 'metrics_tags_idx', 'metrics_idx', 'retentions_idx',
        'tasks', 'task_queue', 'leases'
    ] as Set
    def expectedTypes = ['aggregation_template', 'aggregate_data', 'trigger_def', ] as Set

    def actualTables = getTables(keyspace) as Set
    def actualTypes = getUDTs(keyspace) as Set

    if (actualTables != expectedTables) {
      throw new RuntimeException("The schema is in an unknown state and cannot be versioned. Expected tables to be " +
        "$expectedTables but found $actualTables.")
    }
    if (actualTypes != expectedTypes) {
      throw new RuntimeException("The schema is in an unknown state and cannot be versioned. Expected user defined " +
        "types to be $expectedTypes but found $actualTypes")
    }
  }
} else {
  // If the schema does not already exist, we bypass cassalog's API and create it without
  // being versioned. This allows to use the same logic for subsequent schema changes
  // regardless of whether we are dealing with a new install or an upgrade.

  if (reset) {
    executeCQL("DROP KEYSPACE IF EXISTS $keyspace", 20000)
  }

  executeCQL("CREATE KEYSPACE $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
  executeCQL("USE $keyspace")

  // The retentions map entries are {metric type, retention} key/value paris where
  // retention is specified in days
  executeCQL("""
CREATE TABLE tenants (
    id text PRIMARY KEY,
    retentions map<text, int>
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
""")

  executeCQL("""
CREATE TYPE aggregation_template (
    type int,
    src text,
    interval text,
    fns set<text>
)
""")

  executeCQL("""
CREATE TABLE tenants_by_time (
  bucket timestamp,
    tenant text,
    PRIMARY KEY (bucket, tenant)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
""")

  executeCQL("""
CREATE TYPE aggregate_data (
    type text,
    value double,
    time timeuuid,
    src_metric text,
    src_metric_interval text
)
""")

  executeCQL("""
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

  executeCQL("""
CREATE TABLE metrics_tags_idx (
    tenant_id text,
    tname text,
    tvalue text,
    type tinyint,
    metric text,
    PRIMARY KEY ((tenant_id, tname), tvalue, type, metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
""")

  executeCQL("""
CREATE TABLE metrics_idx (
     tenant_id text,
    type tinyint,
    metric text,
    tags map<text, text>,
    data_retention int,
    PRIMARY KEY ((tenant_id, type), metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
""")

  executeCQL("""
CREATE TABLE retentions_idx (
    tenant_id text,
    type tinyint,
    metric text,
    retention int,
    PRIMARY KEY ((tenant_id, type), metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
""")

  executeCQL("""
CREATE TYPE trigger_def (
    type int,
    trigger_time bigint,
    delay bigint,
    interval bigint,
    repeat_count int,
    execution_count int
)
""")

  executeCQL("""
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

 executeCQL("""
CREATE TABLE leases (
    time_slice timestamp,
    shard int,
    owner text,
    finished boolean,
    PRIMARY KEY (time_slice, shard)
)
""")
}