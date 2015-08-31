#!/bin/bash
#
# Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
# and other contributors as indicated by the @author tags.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Check differences between JAX-RS implementations before commit. If any new difference is introduced, update the
# diff file (thus it will show up on GitHub PR views).
#
# Metrics developers should install this file in <metrics-repo-local-copy>/.git/hooks and make sure the file is
# executable.
#

diff -r --exclude=target api/metrics-api-jaxrs api/metrics-api-jaxrs-1.1 > api/diff.txt

MODIFIED=$(git status --porcelain api/diff.txt | wc -l)

if [ ${MODIFIED} -eq 1 ]; then
  git add api/diff.txt
fi
