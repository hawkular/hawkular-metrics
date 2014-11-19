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

-- The retentions map entries are <metric_type, interval> --> retention

CREATE TABLE ${keyspace}.tenants (
    id text PRIMARY KEY,
    retentions map<frozen<tuple<int, text>>, int>,
    aggregation_templates list<frozen<aggregation_template>>
);

-- #

CREATE TYPE ${keyspace}.aggregate_data (
    type text,
    value double,
    time timeuuid,
    src_metric text,
    src_metric_interval text
);

-- #

CREATE TABLE ${keyspace}.data (
    tenant_id text,
    type int,
    metric text,
    interval text,
    dpart bigint,
    time timeuuid,
    attributes map<text, text> static,
    n_value double,
    availability blob,
    aggregates set<frozen <aggregate_data>>,
    PRIMARY KEY ((tenant_id, type, metric, interval, dpart), time)
)
WITH CLUSTERING ORDER BY (time DESC);

-- #

CREATE TABLE ${keyspace}.counters (
    tenant_id text,
    group text,
    c_name text,
    c_value counter,
    PRIMARY KEY ((tenant_id, group), c_name)
);

-- #

CREATE TABLE ${keyspace}.tags (
    tenant_id text,
    tag text,
    type int,
    metric text,
    interval text,
    time timeuuid,
    n_value double,
    aggregates set<frozen <aggregate_data>>,
    availability blob,
    PRIMARY KEY ((tenant_id, tag, type), metric, interval, time)
);