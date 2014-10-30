-- This CQL file is used to create the schema temporarily until we get a better schema
-- change management tool in place. The file is parsed and executed by SchemaManger.
--
-- NOTE: Statements must must be preceded by -- #
--
-- Comments that start with a double dash like this are suppressed.

-- #

CREATE KEYSPACE rhq WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}

-- #

CREATE TYPE rhq.aggregation_template (
    type text,
    src text,
    interval text,
    fns set<text>
);

-- #

CREATE TABLE rhq.tenants (
    id text PRIMARY KEY,
    retentions map<frozen<tuple<text, text>>, int>,
    aggregation_templates list<frozen<aggregation_template>>
);

-- #

CREATE TYPE rhq.aggregate_data (
    type text,
    value double,
    time timeuuid,
    src_metric text,
    src_metric_interval text
);

-- #

CREATE TABLE rhq.numeric_data (
    tenant_id text,
    metric text,
    interval text,
    dpart bigint,
    time timeuuid,
    attributes map<text, text> static,
    raw double,
    aggregates set<frozen <aggregate_data>>,
    PRIMARY KEY ((tenant_id, metric, interval, dpart), time)
)
WITH CLUSTERING ORDER BY (time DESC);

-- #

CREATE TABLE rhq.availability (
    tenant_id text,
    type text,
    metric text,
    dpart text,
    time timeuuid,
    attributes map<text, text> static,
    value blob,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
);

-- #

CREATE TABLE rhq.tags (
    tenant_id text,
    tag text,
    type text,
    metric text,
    time timeuuid,
    raw_data double,
    aggregates set<frozen <aggregate_data>>,
    availability blob,
    PRIMARY KEY ((tenant_id, tag), type, metric, time)
);