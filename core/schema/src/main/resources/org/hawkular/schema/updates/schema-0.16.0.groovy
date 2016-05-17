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
  version '2.0'
  author 'jsanda'
  tags '0.16.x'
  cql """
CREATE TABLE locks (
    name text PRIMARY KEY,
    owner text,
    owners list<text>
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
"""
}

schemaChange {
  version '2.1'
  author 'jsanda'
  tags '0.16.x'
  cql """
CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    type text,
    name text,
    params map<text, text>,
    trigger frozen <trigger_def>
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
"""
}

schemaChange {
  version '2.2'
  author 'jsanda'
  tags '0.16.x'
  cql """
CREATE TABLE jobs_status (
    time_slice timestamp,
    job_id uuid,
    status text,
    PRIMARY KEY (time_slice, job_id)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
"""
}

schemaChange {
  version '2.3'
  author 'jsanda'
  tags '0.16.x'
  cql """
CREATE TABLE finished_jobs_time_idx (
    time_slice timestamp,
    job_id uuid,
    PRIMARY KEY (time_slice, job_id)
) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
"""
}