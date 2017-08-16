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
  version '8.0'
  author 'jsanda'
  tags '0.27.x'
  description """
This is an event log used to track interesting events in the Cassandra cluster for which maintenance may need to be
performed. See HWKMETRICS-546 and HWKMETRICS-548 for more information.
"""
  cql """
  CREATE TABLE cassandra_mgmt_history (
    bucket timestamp,
    time timestamp,
    event smallint,
    details map<text,text>,
    PRIMARY KEY (bucket, time)
  )
"""
}