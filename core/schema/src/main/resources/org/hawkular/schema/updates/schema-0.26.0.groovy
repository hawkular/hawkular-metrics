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

schemaChange {
  version '7.0'
  author 'snegrea'
  tags '0.26.x'
  cql """
  CREATE TABLE metrics_expiration_idx (
    tenant_id text,
    type tinyint,
    metric text,
    time timestamp,
    PRIMARY KEY ((tenant_id, type), metric)
  )
"""
  verify { tableExists(keyspace, 'metrics_expiration_idx') }
}
