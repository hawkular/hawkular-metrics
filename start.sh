#!/bin/bash

# set -x

BACKEND=mem
# uncomment the next line if you want to use an already running Cassandra server for the backend.
# BACKEND=cass

if [ x$1 == "xcass" -o x$1 == "xmem" ]
then
    BACKEND=$1
fi

WFLY_VERSION=`grep "<version.wildfly>" pom.xml | sed -E 's/^.*y>(8.*l)<.*$/\1/'`

if [ ! -e core-api/target/rhq-metrics-api*.jar ]
then
    cd core-api
    mvn install
    cd ..
fi

if [ ! -e metrics-core/target/rhq-metrics-core*.jar ]
then
    cd metrics-core
    mvn install
    cd ..
fi

if [ ! -e rest-servlet/target/rhq-metric-rest*.war ]
then
    cd rest-servlet
    mvn install
    cd ..
fi

if [ ! -e console/target/metrics-console*.war ]
then
    cd console
    mvn install
    cd ..
fi

if [ ! -e target ]
then
    mkdir target
fi

if [ ! -e $HOME/.m2/repository/org/wildfly/wildfly-dist/$WFLY_VERSION/wildfly-dist-$WFLY_VERSION.zip ]
then
    echo "Downloading WildFly $WFLY_VERSION into local maven repository"
    mvn -P download_wildfly -DskipTests install
fi

if [ ! -e target/wild* ]
then
    cd target
    unzip $HOME/.m2/repository/org/wildfly/wildfly-dist/$WFLY_VERSION/wildfly-dist-$WFLY_VERSION.zip
    cd ..
fi

cp rest-servlet/target/rhq-metric-rest*.war target/wildfly-$WFLY_VERSION/standalone/deployments/
cp console/target/metrics-console-*.war target/wildfly-$WFLY_VERSION/standalone/deployments/
target/wildfly-$WFLY_VERSION/bin/standalone.sh -Drhq-metrics.backend=$BACKEND --debug 8787
