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
  version '6.0'
  author 'jsanda'
  tags '0.23.x'
  cql """
ALTER TABLE data_compressed WITH compaction = {
  'class': 'TimeWindowCompactionStrategy',
  'compaction_window_unit': 'DAYS',
  'compaction_window_size': '1'
}
"""
}

schemaChange {
  version '6.1'
  author 'jsanda'
  tags '0.23.x'
  cql "ALTER TABLE data WITH compaction = {'class': 'SizeTieredCompactionStrategy'}"
}