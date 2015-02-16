#!/bin/bash -e
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
   --backend=cass|embedded_cass                     [OPTIONAL]
      Backend type to be used by the deployment.
      'cass'         = connects to a cassandra cluster
      'embedded_cass' = use and connect to the built-in embedded cassandra server
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
   BACKEND="embedded_cass"
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
# Description: Prepares Wildfly archive
#========================================================================================
prepare_wildfly()
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
      <groupId>org.wildfly</groupId>
      <artifactId>wildfly-dist</artifactId>
      <version>{version}</version>
      <type>zip</type>
    </dependency>
  </dependencies>
</project>
EOF
)

   local WILDFLY_VERSION=$1

   TEMPLATE=`echo $TEMPLATE | sed "s/{version}/$WILDFLY_VERSION/g"`

   echo $TEMPLATE > wildfly.xml

   mvn dependency:list -f wildfly.xml -q
   if [ $? -ne 0 ]
   then
      clean_temp_files
      echo "WILDFLY version $1 not found!"
      exit 1
   fi

   clean_temp_files
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

   local POM_VERSION=`cat output.txt | grep "$4" | cut -d":" -f4`
   clean_temp_files

   echo $POM_VERSION
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


if [ $DEV == false ]
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

   METRICS_CONSOLE_VERSION=`grep "<version>" pom.xml | head -n 1| cut -d">" -f2 | cut -d"<" -f1`
   echo "RHQ Metrics Console: ${METRICS_CONSOLE_VERSION}"

   REST_SERVLET_VERSION=`grep "<version>" pom.xml | head -n 1| cut -d">" -f2 | cut -d"<" -f1`
   echo "RHQ REST: ${REST_SERVLET_VERSION}"

   EXPLORER_VERSION=`grep "<version>" pom.xml | head -n 1| cut -d">" -f2 | cut -d"<" -f1`
   echo "RHQ Explorer: ${EXPLORER_VERSION}"

   EMBEDDED_CASSANDRA_VERSION=`grep "<version>" pom.xml | head -n 1| cut -d">" -f2 | cut -d"<" -f1`
   echo "Embedded Cassandra: ${EMBEDDED_CASSANDRA_VERSION}"

   mvn install -DskipTests ${MVN_SETTINGS_OPT}
   if [ $? -ne 0 ]
   then
      exit
   fi
fi

if [ ! -e target ]
then
    mkdir target
fi

if [ $DEV == false ];
then
   WFLY_VERSION="8.2.0.Final"
   prepare_wildfly $WFLY_VERSION
else
   WFLY_VERSION=`grep "<version.wildfly>" pom.xml | cut -d">" -f2 | cut -d"<" -f1`
fi

WFLY_ZIP=${MVN_REPO}/org/wildfly/wildfly-dist/${WFLY_VERSION}/wildfly-dist-${WFLY_VERSION}.zip

if [ ! -e target/wild* ]
then
    cd target
    unzip ${WFLY_ZIP}
    cd ..
else
    rm -f target/wildfly-${WFLY_VERSION}/standalone/deployments/*
fi

if [ "$REST_SERVLET_VERSION" != "NOT_FOUND" ]
then
   cp ${MVN_REPO}/org/rhq/metrics/rest-servlet/${REST_SERVLET_VERSION}/rest-servlet-${REST_SERVLET_VERSION}.war target/wildfly-${WFLY_VERSION}/standalone/deployments/
fi

if [ "$METRICS_CONSOLE_VERSION" != "NOT_FOUND" ]
then
   cp ${MVN_REPO}/org/rhq/metrics/metrics-console/${METRICS_CONSOLE_VERSION}/metrics-console-${METRICS_CONSOLE_VERSION}.war target/wildfly-${WFLY_VERSION}/standalone/deployments/
fi

if [ "$EXPLORER_VERSION" != "NOT_FOUND" ]
then
   cp ${MVN_REPO}/org/rhq/metrics/explorer/${EXPLORER_VERSION}/explorer-${EXPLORER_VERSION}.war target/wildfly-${WFLY_VERSION}/standalone/deployments/
fi

if [ "$EMBEDDED_CASSANDRA_VERSION" != "NOT_FOUND" ]
then
   cp ${MVN_REPO}/org/rhq/metrics/embedded-cassandra-ear/${EMBEDDED_CASSANDRA_VERSION}/embedded-cassandra-ear-${EXPLORER_VERSION}.ear target/wildfly-${WFLY_VERSION}/standalone/deployments/
fi

target/wildfly-${WFLY_VERSION}/bin/standalone.sh -Drhq-metrics.backend=${BACKEND} --debug 8787 -b ${BIND_ADDRESS}
