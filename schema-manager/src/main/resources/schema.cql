--
-- Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
-- and other contributors as indicated by the @author tags.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- This CQL file is used to create the schema temporarily until we get a better schema
-- change management tool in place. The file is parsed and executed by SchemaManger.
--
-- NOTE: Statements must must be preceded by -- #
--
-- Comments that start with a double dash like this are suppressed.

-- #

CREATE KEYSPACE ${keyspace} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}

-- #

CREATE TYPE ${keyspace}.aggregation_template (
    type int,
    src text,
    interval text,
    fns set<text>
);

-- #

-- The retentions map entries are {metric type, retention} key/value pairs
-- where retention is specified in days.

CREATE TABLE ${keyspace}.tenants (
    id text PRIMARY KEY,
    retentions map<text, int>
) WITH compaction = { 'class': 'LeveledCompactionStrategy' };

-- #

CREATE TABLE ${keyspace}.tenants_by_time (
    bucket timestamp,
    tenant text,
    PRIMARY KEY (bucket, tenant)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' };

-- #

CREATE TYPE ${keyspace}.aggregate_data (
    type text,
    value double,
    time timeuuid,
    src_metric text,
    src_metric_interval text
);

-- #

-- The type column identifies the type of metric. We currently only support
-- gauge and availability. More types may be added in the future. For gauge
-- metrics the n_value column will be set, and the availability column will not
-- be set. For availability metrics the availability column will be set, and
-- the n_value column will not be set.
--
-- The m_tags column is a map of tags that apply to the metric whereas the tags
-- column is a map of tags for individual data points.
--
-- The dpart column is used for bucketing data. For example, we might decide
-- that a partition should store no more than a day's worth of data. dpart
-- would then be rounded down to the start time of each day. dpart is currently
-- set to zero because it is not yet supported. We still have to determine what
-- sensible bucket sizes are.

CREATE TABLE ${keyspace}.data (
    tenant_id text,
    type int,
    metric text,
    interval text,
    dpart bigint,
    time timeuuid,
    data_retention int static,
    m_tags map<text, text> static,
    n_value double,
    availability blob,
    l_value bigint,
    aggregates set<frozen <aggregate_data>>,
    tags map<text, text>,
    PRIMARY KEY ((tenant_id, type, metric, interval, dpart), time)
)
WITH CLUSTERING ORDER BY (time DESC);

-- #

CREATE TABLE ${keyspace}.metrics_tags_idx (
    tenant_id text,
    tname text,
    tvalue text,
    type int,
    metric text,
    interval text,
    PRIMARY KEY ((tenant_id, tname), tvalue, type, metric, interval)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' };

-- #

CREATE TABLE ${keyspace}.tags (
    tenant_id text,
    tname text,      // the tag name
    tvalue text,     // the tag value
    type int,
    metric text,
    interval text,
    time timeuuid,
    n_value double,
    aggregates set<frozen <aggregate_data>>,
    availability blob,
    PRIMARY KEY ((tenant_id, tname), tvalue, type, metric, interval, time)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' };

-- #

CREATE TABLE ${keyspace}.metrics_idx (
    tenant_id text,
    type int,
    metric text,
    interval text,
    tags map<text, text>,
    data_retention int,
    PRIMARY KEY ((tenant_id, type), interval, metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' };

-- #

-- We also store tenant-level retentions in this table. They will be stored using
-- reserved characters, e.g., $gauge, $availability. The retention is stored in
-- days.

CREATE TABLE ${keyspace}.retentions_idx (
    tenant_id text,
    type int,
    metric text,
    retention int,
    PRIMARY KEY ((tenant_id, type), metric)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' };

-- #

-- Task scheduler schema

CREATE TYPE ${keyspace}.trigger_def (
    type int,
    trigger_time bigint,
    delay bigint,
    interval bigint,
    repeat_count int,
    execution_count int
);

-- #

CREATE TABLE ${keyspace}.tasks (
    id uuid,
    group_key text,
    name text,
    exec_order int,
    params map<text, text>,
    trigger frozen <trigger_def>,
    PRIMARY KEY (id)
);

-- #

CREATE TABLE ${keyspace}.task_queue (
    time_slice timestamp,
    shard int,
    group_key text,
    exec_order int,
    task_name text,
    task_id uuid,
    task_params map<text, text>,
    trigger frozen <trigger_def>,
    PRIMARY KEY ((time_slice, shard), group_key, exec_order, task_name, task_id)
);

-- #

CREATE TABLE ${keyspace}.leases (
    time_slice timestamp,
    shard int,
    owner text,
    finished boolean,
    PRIMARY KEY (time_slice, shard)
);