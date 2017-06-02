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

/*
schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_0 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}

schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_1 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}

schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_2 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}
schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_3 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}
schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_4 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}
schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_5 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}
schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_6 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}
schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_7 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}
schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_8 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}
schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_9 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}

schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_10 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}

schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
CREATE TABLE data_temp_11 (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,,
    n_value double,
    availability blob,
    l_value bigint,
    tags map<text,text>,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
}

// Lets hope this will never be compacted.. just to be on the safe-side though..
schemaChange {
  version '8.0'
  author 'burmanm'
  tags '0.27.x'
  cql """
ALTER TABLE data_temp_0 WITH compaction = {
  'class': 'TimeWindowCompactionStrategy',
  'compaction_window_unit': 'DAYS',
  'compaction_window_size': '1'
}
"""
}
*/