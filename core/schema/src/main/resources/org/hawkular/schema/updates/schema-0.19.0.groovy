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

setKeyspace keyspace

schemaChange {
  version '3.0'
  author 'jsanda'
  tags '0.18.x', '0.19.x'
  cql """
ALTER TABLE data WITH COMPRESSION = {'sstable_compression': 'DeflateCompressor'};
"""
}

schemaChange {
  version '3.1'
  author 'jsanda'
  tags '0.19.x'
  cql """
CREATE TABLE rollup60 (
    tenant_id text,
    metric text,
    shard  bigint,
    time timestamp,
    min double,
    max double,
    avg double,
    median double,
    sum double,
    samples int,
    percentiles frozen <map<float, double>>,
    PRIMARY KEY ((tenant_id, metric, shard), time)
)
"""
}

schemaChange {
  version '3.2'
  author 'jsanda'
  tags '0.19.x'
  cql """
CREATE TABLE rollup300 (
    tenant_id text,
    metric text,
    shard  bigint,
    time timestamp,
    min double,
    max double,
    avg double,
    median double,
    sum double,
    samples int,
    percentiles frozen <map<float, double>>,
    PRIMARY KEY ((tenant_id, metric, shard), time)
)
"""
}

schemaChange {
  version '3.2'
  author 'jsanda'
  tags '0.19.x'
  cql """
CREATE TABLE rollup3600 (
    tenant_id text,
    metric text,
    shard  bigint,
    time timestamp,
    min double,
    max double,
    avg double,
    median double,
    sum double,
    samples int,
    percentiles frozen <map<float, double>>,
    PRIMARY KEY ((tenant_id, metric, shard), time)
)
"""
}
