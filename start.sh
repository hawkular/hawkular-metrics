#!/bin/bash

# Usage start.sh [mem|cass|em_cass] [bind_addr]

# set -x

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
      Backend type to be used by the deployment. 'mem' uses the built-in memory enginer, 'cass' connects to a cassandra cluster.
      Script default is 'mem'.
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
   BACKEND='mem'
   VERSION=
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
               "")
                  BACKEND="cass"
                  shift
                  ;;
               *)
                  BACKEND="cass"
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


parse_and_validate_options $@


# Use the path to maven settings.xml file from the environment if available
if [ -n "${MVN_SETTINGS}" ]
then
    MVN_SETTINGS_OPT="-s \"${MVN_SETTINGS}\""
else
    MVN_SETTINGS_OPT=""
fi

if [ x$1 == "xcass" -o x$1 == "xmem" ]
then
    BACKEND=$1
    shift
fi


MVN_REPO=`mvn help:evaluate -Dexpression=settings.localRepository ${MVN_SETTINGS_OPT} | grep -vF "[INFO]" | tail -1`


if [ $DEV == false ];
then
    MAVEN_DEPENDENCY_OUTPUT=`mvn dependency:list -f start.xml`

    METRICS_CONSOLE_VERSION=`echo "${MAVEN_DEPENDENCY_OUTPUT}" | grep 'org.rhq.metrics:metrics-console:war' | cut -d':' -f4`
    REST_SERVLET_VERSION=`echo "${MAVEN_DEPENDENCY_OUTPUT}" | grep 'org.rhq.metrics:rest-servlet:war' | cut -d':' -f4`
    EXPLORER_VERSION=`echo "${MAVEN_DEPENDENCY_OUTPUT}" | grep 'org.rhq.metrics:explorer:war' | cut -d':' -f4`
else
    METRICS_CONSOLE_VERSION=`grep "<version>" pom.xml | head -n 1| awk -F '(<|>)' '{print $3}'`
    REST_SERVLET_VERSION=`grep "<version>" pom.xml | head -n 1| awk -F '(<|>)' '{print $3}'`
    EXPLORER_VERSION=`grep "<version>" pom.xml | head -n 1| awk -F '(<|>)' '{print $3}'`

    mvn install -DskipTests ${MVN_SETTINGS_OPT}
    if [ $? -ne 0 ]
    then
        exit;
    fi
fi

echo 'Using: '
echo 'RHQ Metrics Console: ${METRICS_CONSOLE_VERSION}'
echo 'RHQ REST: ${REST_SERVLET_VERSION}'
echo 'RHQ Explorer: ${EXPLORER_VERSION}'

if [ ! -e target ]
then
    mkdir target
fi



WFLY_VERSION=`grep "<version.wildfly>" pom.xml | sed -E 's/^.*y>(8.*l)<.*$/\1/'`
WFLY_ZIP=${MVN_REPO}/org/wildfly/wildfly-dist/${WFLY_VERSION}/wildfly-dist-${WFLY_VERSION}.zip

if [ ! -e target/wild* ]
then
    cd target
    unzip ${WFLY_ZIP}
    cd ..
else
    rm target/wildfly-${WFLY_VERSION}/standalone/deployments/*
fi

cp ${MVN_REPO}/org/rhq/metrics/rest-servlet/${REST_SERVLET_VERSION}/rest-servlet-${REST_SERVLET_VERSION}.war target/wildfly-${WFLY_VERSION}/standalone/deployments/
cp ${MVN_REPO}/org/rhq/metrics/metrics-console/${METRICS_CONSOLE_VERSION}/metrics-console-${METRICS_CONSOLE_VERSION}.war target/wildfly-${WFLY_VERSION}/standalone/deployments/
cp ${MVN_REPO}/org/rhq/metrics/explorer/${EXPLORER_VERSION}/explorer-${EXPLORER_VERSION}.war target/wildfly-${WFLY_VERSION}/standalone/deployments/
target/wildfly-${WFLY_VERSION}/bin/standalone.sh -Drhq-metrics.backend=${BACKEND} --debug 8787 -b ${BIND_ADDRESS}
