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

schemaChange {
  version '3.0'
  author 'jsanda'
  tags '0.18.x', '0.19.x'
  cql """
ALTER TABLE ${keyspace}.data WITH COMPRESSION = {'sstable_compression': 'DeflateCompressor'};
"""
}

schemaChange {
  version '3.1'
  author 'jsanda'
  tags '0.19.x'
  cql "ALTER TABLE data WITH compaction = {'class': 'DateTieredCompactionStrategy'} AND default_time_to_live = 604800"
}

schemaChange {
  version '3.2'
  author 'snegre'
  tags '0.19.x'
  cql "ALTER TABLE data DROP aggregates"
  verify { columnDoesNotExist(keyspace, 'data', 'aggregates') }
}

schemaChange {
  version '3.3'
  author 'snegrea'
  tags '0.19.x'
  cql "DROP TYPE aggregate_data"
  verify { typeDoesNotExist(keyspace, 'aggregate_data') }
}


schemaChange {
  version '3.4'
  author 'snegrea'
  tags '0.19.x'
  cql "ALTER TABLE data DROP data_retention"
  verify { columnDoesNotExist(keyspace, 'data', 'data_retention') }
}

schemaChange {
  version '3.5'
  author 'snegrea'
  tags '0.19.x'
  cql "DROP TYPE aggregation_template"
  verify { typeDoesNotExist(keyspace, 'aggregation_template') }
}

schemaChange {
  version '3.6'
  author 'snegrea'
  tags '0.19.x'
  cql "DROP TABLE tenants_by_time"
  verify { tableDoesNotExist(keyspace, 'tenants_by_time') }
}
