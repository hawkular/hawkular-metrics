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

// Since Hawkular Metrics 0.8.0 was shipped in OpenShift 3.1, we need to layer any changes
// on top of that schema.
if (appVersion == '0.8.0') {
  schemaChange {
    version 'drop aggregation_template'
    author 'jsanda'
    cql "DROP TYPE aggregation_template"
  }

  schemaChange {
    version 'drop tenants_by_time'
    author 'jsanda'
    cql "DROP TABLE tenants_by_time"
  }

  schemaChange {
    version 'drop column data.aggregates'
    author 'jsanda'
    cql "ALTER TABLE data DROP aggregates"
  }

  schemaChange {
    version 'drop type aggregate_data'
    author 'jsanda'
    cql "DROP TYPE aggregate_data"
  }

//  schemaChange {
//    version 'drop table tasks'
//    author 'jsanda'
//    cql "DROP TABLE tasks"
//  }
//
//  schemaChange {
//    version 'drop table task_queue'
//    author 'jsanda'
//    cql "DROP TABLE task_queue"
//  }
//
//  schemaChange {
//    version 'drop table leases'
//    author 'jsanda'
//    cql "DROP TABLE leases"
//  }
//
//  schemaChange {
//    version 'drop type trigger_def'
//    author 'jsanda'
//    cql "DROP TYPE trigger_def"
//  }
} else {
  createKeyspace {
    version "create-keyspace $keyspace"
    name keyspace
    author 'jsanda'
    recreate reset
    tags 'legacy'
  }

  schemaChange {
    version 'create-table tenants'
    author 'jsanda'
    tags 'legacy'
    cql """
-- The retentions map entries are {metric type, retention} key/value pairs
-- where retention is specified in days.

CREATE TABLE tenants (
    id text PRIMARY KEY,
    retentions map<text, int>
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
"""
  }

  schemaChange {
    version 'create-table data'
    author 'jsanda'
    tags 'legacy'
    cql """
-- The type column identifies the type of metric. We currently only support
-- gauge and availability. More types may be added in the future. For gauge
-- metrics the n_value column will be set, and the availability column will not
-- be set. For availability metrics the availability column will be set, and
-- the n_value column will not be set.
--
-- tags column is a map of tags for individual data points.
--
-- The dpart column is used for bucketing data. For example, we might decide
-- that a partition should store no more than a day's worth of data. dpart
-- would then be rounded down to the start time of each day. dpart is currently
-- set to zero because it is not yet supported. We still have to determine what
-- sensible bucket sizes are.

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
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
)
WITH CLUSTERING ORDER BY (time DESC)
"""
  }

  schemaChange {
    version 'create-table metrics_tags_idx'
    author 'jsanda'
    tags 'legacy'
    cql """
CREATE TABLE metrics_tags_idx (
    tenant_id text,
    tname text,
    tvalue text,
    type tinyint,
    metric text,
    PRIMARY KEY ((tenant_id, tname), tvalue, type, metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
"""
  }

  schemaChange {
    version 'create-table metrics_idx'
    author 'jsanda'
    tags 'legacy'
    cql """
CREATE TABLE metrics_idx (
    tenant_id text,
    type tinyint,
    metric text,
    tags map<text, text>,
    data_retention int,
    PRIMARY KEY ((tenant_id, type), metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
"""
  }

  schemaChange {
    version 'create-table retentions_idx'
    author 'jsanda'
    tags 'legacy'
    cql """
-- We also store tenant-level retentions in this table. They will be stored using
-- reserved characters, e.g., \$gauge, \$availability. The retention is stored in
-- days.

CREATE TABLE retentions_idx (
    tenant_id text,
    type tinyint,
    metric text,
    retention int,
    PRIMARY KEY ((tenant_id, type), metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
"""
  }
}

