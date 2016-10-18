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
  version '4.0'
  author 'burmanm'
  tags '0.20.x'
  cql """
CREATE TABLE data_compressed (
    tenant_id text,
    type tinyint,
    metric text,
    dpart bigint,
    time timestamp,
    c_value blob,
    ts_value blob,
    tags blob,
    PRIMARY KEY ((tenant_id, type, metric, dpart), time)
) WITH CLUSTERING ORDER BY (time DESC)
"""
  verify { tableExists(keyspace, 'data_compressed') }
}

schemaChange {
  version '4.1'
  author 'burmanm'
  tags '0.20.x'
  cql """
ALTER TABLE data WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor'};
"""
}
