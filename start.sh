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

#========================================================================================
# Description: Display an error message and abort the script.
#========================================================================================
abort()
{
   echo >&2
   for ARG in "$@"; do
      echo "$ARG" >&2
      echo "">&2
   done
   exit 1
}

#========================================================================================
# Description: Display usage information then abort the script.
#========================================================================================
usage()
{
      USAGE=$(
cat << EOF
USAGE:   start.sh OPTIONS
   --backend=mem|cass                     [OPTIONAL]
      Backend type to be used by the deployment.
      'mem'          = use built-in memory enginer
      'cass'         = connects to a cassandra cluster
      'embedded_cass = use and connect to the built-in embedded cassandra server
      Script default is 'embedded_cass'.
   --version=version                      [OPTIONAL]
      Released version to be downloaded and used, only applicable if this script is run in 'release' mode.
      Script default is the latest published version.
   --bind-address                         [OPTIONAL]
      Set bind address for the underlying container. Defaults to 0.0.0.0
   --dev                                  [OPTIONAL]
      Set script mode to dev: build from source and use the artifacts just built. 'version' will be ignored.
EOF
)
   EXAMPLE=$(
cat << EOF
EXAMPLE:
   start.sh --backend="cass" --mode="release"
EOF
)
   abort "$@" "$USAGE" "$EXAMPLE"
}

#========================================================================================
# Description: Validate and parse input arguments
#========================================================================================
parse_and_validate_options()
{
   BACKEND="mem"
   VERSION="0.2.4"
   VERSION_REQUESTED=false
   DEV=false
   BIND_ADDRESS=0.0.0.0

   short_options="h"
   long_options="help,version:,backend:,bind-address:,dev"

   PROGNAME=${0##*/}
   ARGS=$(getopt -s bash --options $short_options --longoptions $long_options --name $PROGNAME -- "$@" )
   eval set -- "$ARGS"

   while true; do
       case $1 in
         -h|--help)
            usage
            ;;
         --version)
            shift
            RELEASE_VERSION="$1"
            VERSION_REQUESTED=true
            shift
            ;;
         --backend)
            shift
            case "$1" in
               "mem")
                  BACKEND="mem"
                  shift
                  ;;
               "cass")
                  BACKEND="cass"
                  shift
                  ;;
               "embedded_cass")
                  BACKEND="embedded_cass"
                  shift
                  ;;
               "")
                  BACKEND="embedded_cass"
                  shift
                  ;;
               *)
                  BACKEND="embedded_cass"
                  shift
                  ;;
            esac
            ;;
         --bind-address)
            shift
            BIND_ADDRESS="$1"
            shift
            ;;
         --dev)
            DEV=true
            shift
            ;;
         --)
            shift
            break
            ;;
         *)
            usage
            ;;
       esac
   done
}

#========================================================================================
# Description: Prepares Wildfly/Keycloak archive
#========================================================================================
prepare_keycloak()
{
  ./install-keycloak.sh
  if [ $? -ne 0 ] ; then
    echo "Failed to install Wildfly and Keycloak. Aborting."
    exit -1
  fi
}

#========================================================================================
# Description: Creates a pom.xml file to download the project binaries
#========================================================================================
create_build_pom_file()
{
   local TEMPLATE=$(
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
      <groupId>org.rhq.metrics</groupId>
      <artifactId>{dependency}</artifactId>
      <version>{version}</version>
      <type>{type}</type>
    </dependency>
  </dependencies>
</project>
EOF
)

   local DEPENDENCY=$1
   local DEPENDENCY_VERSION=$2
   local ARCHIVE_TYPE=$3

   TEMPLATE=`echo $TEMPLATE | sed "s/{dependency}/$DEPENDENCY/g"`
   TEMPLATE=`echo $TEMPLATE | sed "s/{version}/$DEPENDENCY_VERSION/g"`
   TEMPLATE=`echo $TEMPLATE | sed "s/{type}/$ARCHIVE_TYPE/g"`

   echo $TEMPLATE > start.xml
}

#========================================================================================
# Description: Cleans temp files created by the script
#========================================================================================
clean_temp_files()
{
  rm -f output.txt
  rm -f start.xml
  rm -f widlfy.txt
}

#========================================================================================
# Description: Downloads the right version of the project artifact
#========================================================================================
get_dependency()
{
   create_build_pom_file $1 $2 $3

   if [ $VERSION_REQUESTED == false ]
   then
      mvn versions:use-latest-releases -DgenerateBackupPoms=false -f start.xml -q > /dev/null
      if [ $? -ne 0 ]
      then
         clean_temp_files
         echo "NOT_FOUND"
         return
      fi
   fi

   mvn dependency:list -f start.xml > output.txt
   if [ $? -ne 0 ]
   then
      clean_temp_files
      echo "NOT_FOUND"
      return
   fi

   local POM_VERSION=`cat output.txt | grep "$4" | cut -d':' -f4`
   clean_temp_files

   echo $POM_VERSION
}

#========================================================================================
# Description: Creates fake Keycloak Realms for development purposes
#========================================================================================
create_fake_realms()
{
   KEY_GENERATION_LOG=/tmp/rhq-metrics-keycloak-key-generation.log
   DELETE_GENERATION_LOG=true
   OPENSSL_BIN=`which openssl 2>/dev/null`
   UUIDGEN_BIN=`which uuidgen 2>/dev/null`
   DELETE_KEY_FILES=true

   TMP_ACME_RRAFFAIRS_PRIV=/tmp/keycloak-acme-roadrunner-affairs.pem
   TMP_ACME_RRAFFAIRS_PUB=/tmp/keycloak-acme-roadrunner-affairs.pub
   TMP_ACME_OTAFFAIRS_PRIV=/tmp/keycloak-acme-other-affairs.pem
   TMP_ACME_OTAFFAIRS_PUB=/tmp/keycloak-acme-other-affairs.pub

   # where to read the templates from
   REALMS_SRC_DIR="`pwd`/realms/"

   # where to write the final files to
   REALMS_DEST_DIR="`pwd`/target/realms/"

   if [ "x$OPENSSL_BIN" == "x" ]
   then
      echo "'openssl' is required in order to generate the private/public key pair for the sample Keycloak realm."
      exit -1
   fi

   if [ "x$UUIDGEN_BIN" == "x" ]
   then
      echo "'uuidgen' is required in order to generate the secret keys for the applications."
      exit -1
   fi

   if [ ! -d "${REALMS_DEST_DIR}" ]
   then
      mkdir -p "${REALMS_DEST_DIR}"
      echo "Generating key pairs and application secrets for Keycloak integration"
      $OPENSSL_BIN genrsa -out $TMP_ACME_RRAFFAIRS_PRIV 1024 >>$KEY_GENERATION_LOG 2>&1
      $OPENSSL_BIN rsa -in $TMP_ACME_RRAFFAIRS_PRIV -pubout > $TMP_ACME_RRAFFAIRS_PUB 2>>$KEY_GENERATION_LOG
      PRIVATE_KEY_REALM_ACME_RRAFFAIRS=$(cat $TMP_ACME_RRAFFAIRS_PRIV | grep -v "PRIVATE KEY" | tr -d '\n')
      PUBLIC_KEY_REALM_ACME_RRAFFAIRS=$(cat $TMP_ACME_RRAFFAIRS_PUB | grep -v "PUBLIC KEY" | tr -d '\n')

      $OPENSSL_BIN genrsa -out $TMP_ACME_OTAFFAIRS_PRIV 1024 >>$KEY_GENERATION_LOG 2>&1
      $OPENSSL_BIN rsa -in $TMP_ACME_OTAFFAIRS_PRIV -pubout > $TMP_ACME_OTAFFAIRS_PUB 2>>$KEY_GENERATION_LOG
      PRIVATE_KEY_REALM_ACME_OTAFFAIRS=$(cat $TMP_ACME_OTAFFAIRS_PRIV | grep -v "PRIVATE KEY" | tr -d '\n')
      PUBLIC_KEY_REALM_ACME_OTAFFAIRS=$(cat $TMP_ACME_OTAFFAIRS_PUB | grep -v "PUBLIC KEY" | tr -d '\n')

      SECRET_METRICS_CONSOLE_RR=`$UUIDGEN_BIN 2>/dev/null`
      SECRET_METRICS_API_RR=`$UUIDGEN_BIN 2>/dev/null`
      SECRET_METRICS_CONSOLE_OT=`$UUIDGEN_BIN 2>/dev/null`
      SECRET_METRICS_API_OT=`$UUIDGEN_BIN 2>/dev/null`

      AGENT_PASSWORD_RRAFFAIRS=`$UUIDGEN_BIN 2>/dev/null`
      AGENT_PASSWORD_OTAFFAIRS=`$UUIDGEN_BIN 2>/dev/null`

      ## replace the placeholders with actual keys
      sed "s|PRIVATE_KEY_REALM_ACME_RRAFFAIRS|$PRIVATE_KEY_REALM_ACME_RRAFFAIRS|g" "$REALMS_SRC_DIR/acme-roadrunner-affairs-realm.template.json" > "$REALMS_DEST_DIR/acme-roadrunner-affairs-realm.json"
      sed -i "s|PUBLIC_KEY_REALM_ACME_RRAFFAIRS|$PUBLIC_KEY_REALM_ACME_RRAFFAIRS|g" "$REALMS_DEST_DIR/acme-roadrunner-affairs-realm.json"
      sed -i "s|SECRET_METRICS_CONSOLE_RR|$SECRET_METRICS_CONSOLE_RR|g" "$REALMS_DEST_DIR/acme-roadrunner-affairs-realm.json"
      sed -i "s|SECRET_METRICS_API_RR|$SECRET_METRICS_API_RR|g" "$REALMS_DEST_DIR/acme-roadrunner-affairs-realm.json"
      sed -i "s|AGENT_PASSWORD_RRAFFAIRS|$AGENT_PASSWORD_RRAFFAIRS|g" "$REALMS_DEST_DIR/acme-roadrunner-affairs-realm.json"

      sed "s|PRIVATE_KEY_REALM_ACME_OTAFFAIRS|$PRIVATE_KEY_REALM_ACME_OTAFFAIRS|g" "$REALMS_SRC_DIR/acme-other-affairs-realm.template.json" > "$REALMS_DEST_DIR/acme-other-affairs-realm.json"
      sed -i "s|PUBLIC_KEY_REALM_ACME_OTAFFAIRS|$PUBLIC_KEY_REALM_ACME_OTAFFAIRS|g" "$REALMS_DEST_DIR/acme-other-affairs-realm.json"
      sed -i "s|SECRET_METRICS_CONSOLE_OT|$SECRET_METRICS_CONSOLE_OT|g" "$REALMS_DEST_DIR/acme-other-affairs-realm.json"
      sed -i "s|SECRET_METRICS_API_OT|$SECRET_METRICS_API_OT|g" "$REALMS_DEST_DIR/acme-other-affairs-realm.json"
      sed -i "s|AGENT_PASSWORD_OTAFFAIRS|$AGENT_PASSWORD_OTAFFAIRS|g" "$REALMS_DEST_DIR/acme-other-affairs-realm.json"

      ## replace placeholders on keycloak.json files as well, used in the frontend
      sed "s|PUBLIC_KEY_REALM_ACME_OTAFFAIRS|$PUBLIC_KEY_REALM_ACME_OTAFFAIRS|g" "$REALMS_SRC_DIR/metrics-console-acme-other-affairs.template.json" > "$REALMS_DEST_DIR/metrics-console-acme-other-affairs.json"
      sed "s|PUBLIC_KEY_REALM_ACME_RRAFFAIRS|$PUBLIC_KEY_REALM_ACME_RRAFFAIRS|g" "$REALMS_SRC_DIR/metrics-console-acme-roadrunner-affairs.template.json" > "$REALMS_DEST_DIR/metrics-console-acme-roadrunner-affairs.json"
      sed "s|PUBLIC_KEY_REALM_ACME_OTAFFAIRS|$PUBLIC_KEY_REALM_ACME_OTAFFAIRS|g" "$REALMS_SRC_DIR/rest-acme-other-affairs.template.json" > "$REALMS_DEST_DIR/rest-acme-other-affairs.json"
      sed "s|PUBLIC_KEY_REALM_ACME_RRAFFAIRS|$PUBLIC_KEY_REALM_ACME_RRAFFAIRS|g" "$REALMS_SRC_DIR/rest-acme-roadrunner-affairs.template.json" > "$REALMS_DEST_DIR/rest-acme-roadrunner-affairs.json"
      sed -i "s|SECRET_METRICS_API_OT|$SECRET_METRICS_API_OT|g" "$REALMS_DEST_DIR/rest-acme-other-affairs.json"
      sed -i "s|SECRET_METRICS_API_RR|$SECRET_METRICS_API_RR|g" "$REALMS_DEST_DIR/rest-acme-roadrunner-affairs.json"
   fi

   echo "Copying the Keycloak's configuration files to the metrics console"
   cp "$REALMS_DEST_DIR/metrics-console-acme-other-affairs.json" ui/console/src/main/webapp/keycloak-acme-other-affairs.json
   cp "$REALMS_DEST_DIR/metrics-console-acme-roadrunner-affairs.json" ui/console/src/main/webapp/keycloak-acme-roadrunner-affairs.json

   echo "Copying the Keycloak's configuration files to the REST API"
   if [ ! -d "rest-servlet/src/main/resources/" ]; then mkdir -p rest-servlet/src/main/resources/ ; fi
   cp "$REALMS_DEST_DIR/rest-acme-other-affairs.json" rest-servlet/src/main/resources/keycloak-acme-other-affairs.json
   cp "$REALMS_DEST_DIR/rest-acme-roadrunner-affairs.json" rest-servlet/src/main/resources/keycloak-acme-roadrunner-affairs.json

   echo "Creating list of realms for Web Console"
   echo '{"realms": ["acme-other-affairs", "acme-roadrunner-affairs"]}' > ui/console/src/main/webapp/realms.json

   if [ "xtrue" == "x$DELETE_GENERATION_LOG" ]
   then
       rm -f $KEY_GENERATION_LOG
   fi

   if [ "xtrue" == "x$DELETE_KEY_FILES" ]
   then
       rm -f $TMP_ACME_RRAFFAIRS_PRIV $TMP_ACME_RRAFFAIRS_PUB $TMP_ACME_OTAFFAIRS_PRIV $TMP_ACME_OTAFFAIRS_PUB
   fi
}


#========================================================================================
# SCRIPT START
#========================================================================================

parse_and_validate_options $@

# Use the path to maven settings.xml file from the environment if available
if [ -n "${MVN_SETTINGS}" ]
then
    MVN_SETTINGS_OPT="-s \"${MVN_SETTINGS}\""
else
    MVN_SETTINGS_OPT=""
fi

MVN_REPO=`mvn help:evaluate -Dexpression=settings.localRepository ${MVN_SETTINGS_OPT} | grep -vF "[INFO]" | tail -1`

if [ ! -e target ]
then
    mkdir target
fi

if [ $DEV == false ];
then
   KC_VERSION="1.1.0.Beta2"
   WILDFLY_VERSION="8.2.0.Final"
else
   KC_VERSION=`mvn help:evaluate -Dexpression=version.keycloak | grep -vE "INFO|Downl"`
   WILDFLY_VERSION=`mvn help:evaluate -Dexpression=version.wildfly | grep -vE "INFO|Downl"`
fi

JBOSS_HOME=$(pwd)/target/wildfly/wildfly-${WILDFLY_VERSION}

if [ ! -d ${JBOSS_HOME} ]
then
    prepare_keycloak $KC_VERSION $WILDFLY_VERSION
else
    for deployment in rest-servlet metrics-console explorer embedded-cassandra-ear ;
    do
      if [ -f ${JBOSS_HOME}/standalone/deployments/${deployment}*war ] ; then
        rm ${JBOSS_HOME}/standalone/deployments/${deployment}*war
      fi
    done
fi

if [ $DEV == false ];
then
   echo "Using: "

   METRICS_CONSOLE_VERSION=$(get_dependency "metrics-console" $VERSION "war" "org.rhq.metrics:metrics-console:war")
   echo "RHQ Metrics Console: ${METRICS_CONSOLE_VERSION}"

   REST_SERVLET_VERSION=$(get_dependency "rest-servlet" $VERSION "war" "org.rhq.metrics:rest-servlet:war")
   echo "RHQ REST: ${REST_SERVLET_VERSION}"

   EXPLORER_VERSION=$(get_dependency "explorer" $VERSION "war" "org.rhq.metrics:explorer:war")
   echo "RHQ Explorer: ${EXPLORER_VERSION}"

   EMBEDDED_CASSANDRA_VERSION=$(get_dependency "embedded-cassandra-ear" $VERSION "ear" "org.rhq.metrics:embedded-cassandra-ear:ear")
   echo "Embedded Cassandra: ${EMBEDDED_CASSANDRA_VERSION}"
else
   echo "Using: "

   METRICS_CONSOLE_VERSION=`grep "<version>" pom.xml | head -n 1| awk -F '(<|>)' '{print $3}'`
   echo "RHQ Metrics Console: ${METRICS_CONSOLE_VERSION}"

   REST_SERVLET_VERSION=`grep "<version>" pom.xml | head -n 1| awk -F '(<|>)' '{print $3}'`
   echo "RHQ REST: ${REST_SERVLET_VERSION}"

   EXPLORER_VERSION=`grep "<version>" pom.xml | head -n 1| awk -F '(<|>)' '{print $3}'`
   echo "RHQ Explorer: ${EXPLORER_VERSION}"

   EMBEDDED_CASSANDRA_VERSION=`grep "<version>" pom.xml | head -n 1| awk -F '(<|>)' '{print $3}'`
   echo "Embedded Cassandra: ${EMBEDDED_CASSANDRA_VERSION}"

   create_fake_realms

   mvn install -DskipTests ${MVN_SETTINGS_OPT}
   if [ $? -ne 0 ]
   then
      exit
   fi
fi

if [ "$REST_SERVLET_VERSION" != "NOT_FOUND" ]
then
   cp ${MVN_REPO}/org/rhq/metrics/rest-servlet/${REST_SERVLET_VERSION}/rest-servlet-${REST_SERVLET_VERSION}.war ${JBOSS_HOME}/standalone/deployments/
fi

if [ "$METRICS_CONSOLE_VERSION" != "NOT_FOUND" ]
then
   cp ${MVN_REPO}/org/rhq/metrics/metrics-console/${METRICS_CONSOLE_VERSION}/metrics-console-${METRICS_CONSOLE_VERSION}.war ${JBOSS_HOME}/standalone/deployments/
fi

if [ "$EXPLORER_VERSION" != "NOT_FOUND" ]
then
   cp ${MVN_REPO}/org/rhq/metrics/explorer/${EXPLORER_VERSION}/explorer-${EXPLORER_VERSION}.war ${JBOSS_HOME}/standalone/deployments/
fi

if [ "$EMBEDDED_CASSANDRA_VERSION" != "NOT_FOUND" ]
then
   cp ${MVN_REPO}/org/rhq/metrics/embedded-cassandra-ear/${EMBEDDED_CASSANDRA_VERSION}/embedded-cassandra-ear-${EXPLORER_VERSION}.ear ${JBOSS_HOME}/standalone/deployments/
fi

KEYCLOAK_FLAGS=""
KEYCLOAK_FLAGS="${KEYCLOAK_FLAGS} -Drhq-metrics.backend=${BACKEND} --debug 8787 -b ${BIND_ADDRESS}"
KEYCLOAK_FLAGS="${KEYCLOAK_FLAGS} -Dkeycloak.import=\"${REALMS_DEST_DIR}/acme-other-affairs-realm.json\",\"${REALMS_DEST_DIR}/acme-roadrunner-affairs-realm.json\""
${JBOSS_HOME}/bin/standalone.sh ${KEYCLOAK_FLAGS}
