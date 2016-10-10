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
  version '5.0'
  author 'jsanda'
  tags '0.21.x'
  cql "ALTER TABLE scheduled_jobs_idx ADD job_name text"
}

schemaChange {
  version '5.1'
  author 'jsanda'
  tags '0.21.x'
  cql "ALTER TABLE scheduled_jobs_idx ADD job_params frozen<map<text, text>>"
}

schemaChange {
  version '5.2'
  author 'jsanda'
  tags '0.21.x'
  cql "ALTER TABLE scheduled_jobs_idx ADD trigger frozen<trigger_def>"
}

schemaChange {
  version '5.3'
  author 'jsanda'
  tags '0.21.x'
  cql "ALTER TABLE scheduled_jobs_idx ADD job_type text"
}

schemaChange {
  version '5.4'
  author 'jsanda'
  tags '0.21.x'
  cql "ALTER TABLE scheduled_jobs_idx ADD status tinyint"
}

schemaChange {
  version '5.5'
  author 'jsanda'
  tags '0.21.x'
  cql "INSERT INTO sys_config (config_id, name, value) VALUES ('org.hawkular.metrics', 'gc_grace_seconds', '604800')"
}
