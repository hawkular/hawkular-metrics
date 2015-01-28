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

if [ -d target/wildfly ] ; then
  echo "Removing the existing Wildfly/Keycloak setup"
  rm -rf target/wildfly
fi

TEMPLATE=$(
cat << EOF
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>

  <groupId>org.rhq.metrics.build</groupId>
  <artifactId>rhq-metrics-build</artifactId>
  <version>0.0.1</version>
  <packaging>pom</packaging>

  <dependencies>
    <dependency>
      <groupId>org.wildfly</groupId>
      <artifactId>wildfly-dist</artifactId>
      <version>{wildfly_version}</version>
      <type>zip</type>
    </dependency>
    <dependency>
      <groupId>org.keycloak</groupId>
      <artifactId>keycloak-wildfly-adapter-dist</artifactId>
      <version>{kc_version}</version>
      <type>zip</type>
    </dependency>
    <dependency>
      <groupId>org.keycloak</groupId>
      <artifactId>keycloak-war-dist-all</artifactId>
      <version>{kc_version}</version>
      <type>zip</type>
      <exclusions>
        <exclusion>
          <groupId>org.keycloak</groupId>
          <artifactId>keycloak-example-themes-dist</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
  </dependencies>
</project>
EOF
)

KC_VERSION=$1
WILDFLY_VERSION=$2

if [ "x$KC_VERSION" == "x" ]; then
  KC_VERSION=`mvn help:evaluate -Dexpression=version.keycloak | grep -vE "INFO|Downl"`
fi

if [ "x$WILDFLY_VERSION" == "x" ]; then
  WILDFLY_VERSION=`mvn help:evaluate -Dexpression=version.wildfly | grep -vE "INFO|Downl"`
fi

echo "Using Keycloak $KC_VERSION and Wildfly $WILDFLY_VERSION"

TEMPLATE=$(echo $TEMPLATE | sed "s/{wildfly_version}/${WILDFLY_VERSION}/g" | sed "s/{kc_version}/${KC_VERSION}/g")
echo $TEMPLATE > wildfly.xml
mvn dependency:list -f wildfly.xml -q
if [ $? -ne 0 ]
then
  rm wildfly.xml
  echo "WILDFLY version $1 not found!"
  exit 1
fi

MVN_REPO=`mvn help:evaluate -Dexpression=settings.localRepository -f wildfly.xml | grep -vF "[INFO]" | tail -1`
WILDFLY_ZIP=${MVN_REPO}/org/wildfly/wildfly-dist/${WILDFLY_VERSION}/wildfly-dist-${WILDFLY_VERSION}.zip
ADAPTER_ZIP="${MVN_REPO}/org/keycloak/keycloak-wildfly-adapter-dist/${KC_VERSION}/keycloak-wildfly-adapter-dist-${KC_VERSION}.zip"
KC_WAR_ZIP="${MVN_REPO}/org/keycloak/keycloak-war-dist-all/${KC_VERSION}/keycloak-war-dist-all-${KC_VERSION}.zip"

mkdir -p target/wildfly
cd target/wildfly
unzip -q $WILDFLY_ZIP
unzip -q $KC_WAR_ZIP
JBOSS_HOME=$(pwd)/wildfly-${WILDFLY_VERSION}

cd $JBOSS_HOME
sed -i -e 's/<extensions>/&\n        <extension module="org.keycloak.keycloak-subsystem"\/>/' $JBOSS_HOME/standalone/configuration/standalone.xml
sed -i -e 's/<profile>/&\n        <subsystem xmlns="urn:jboss:domain:keycloak:1.0"\/>/' $JBOSS_HOME/standalone/configuration/standalone.xml
sed -i -e 's/<security-domains>/&\n                <security-domain name="keycloak">\n                    <authentication>\n                        <login-module code="org.keycloak.adapters.jboss.KeycloakLoginModule" flag="required"\/>\n                    <\/authentication>\n                <\/security-domain>/' $JBOSS_HOME/standalone/configuration/standalone.xml
unzip -qo ${ADAPTER_ZIP}
cd ..

cp -r keycloak-war-dist-all-${KC_VERSION}/deployments/* ${JBOSS_HOME}/standalone/deployments/
cd ../..
rm wildfly.xml
