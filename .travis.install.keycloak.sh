#!/bin/bash
#
# Copyright 2014 Red Hat, Inc. and/or its affiliates
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

if [ -d .keycloak ] ; then rm -rf .keycloak ; fi
mvn clean install -Pdownload_keycloak -N > install-keycloak.log
KC_VERSION=`grep "<version.keycloak>" pom.xml | sed -E 's/^.*k>(1.*)<.*$/\1/'`

echo "Using Keycloak ${KC_VERSION} appliance distribution as application server."
MVN_REPO=`mvn help:evaluate -Dexpression=settings.localRepository -U -N | grep -Ev 'INFO|Download'`
KC_ZIP=${MVN_REPO}/org/keycloak/keycloak-appliance-dist-all/${KC_VERSION}/keycloak-appliance-dist-all-${KC_VERSION}.zip

mkdir .keycloak
cd .keycloak
echo "Preparing Keycloak Appliance distribution"
unzip -q ${KC_ZIP}
