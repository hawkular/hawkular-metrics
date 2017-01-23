#!/bin/bash
#
# Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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


set -xe

MVN_VERSION="$1"
MVN_INSTALL_DIR="$2"

if [ ! -f "${MVN_INSTALL_DIR}/lib/maven-artifact-${MVN_VERSION}.jar" ]; then
  # remove the older version that can eventually be there
  rm -Rf "${MVN_INSTALL_DIR}"
  mkdir -p "${MVN_INSTALL_DIR}"

  # Find the closest Apache mirror
  APACHE_MIRROR="$(curl -sL https://www.apache.org/dyn/closer.cgi?asjson=1 | python -c 'import sys, json; print json.load(sys.stdin)["preferred"]')"
  curl -o "${HOME}/apache-maven-$MVN_VERSION-bin.tar.gz" "$APACHE_MIRROR/maven/maven-3/$MVN_VERSION/binaries/apache-maven-$MVN_VERSION-bin.tar.gz"
  cd "${MVN_INSTALL_DIR}"
  tar -xzf "${HOME}/apache-maven-$MVN_VERSION-bin.tar.gz" --strip 1
  chmod +x "${MVN_INSTALL_DIR}/bin/mvn"
else
  echo "Using cached Maven ${MVN_VERSION}"
fi
${MVN_INSTALL_DIR}/bin/mvn -version
