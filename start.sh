#!/bin/bash

# set -x

BACKEND=mem
# uncomment the next line if you want to use an already running Cassandra server for the backend.
# BACKEND=cass

if [ ! -e core-api/target/rhq-metrics-api*.jar ]
then
    cd core-api
    mvn install
    cd ..
fi

if [ ! -e metrics-server/target/rhq-metrics-server-*.jar ]
then
    cd metrics-server
    mvn install
    cd ..
fi

if [ ! -e dummy-ui/target/dummy-ui*.war ]
then
    cd dummy-ui
    mvn install
    cd ..
fi

if [ ! -e rest-servlet/target/rhq-metric-rest*.war ]
then
    cd rest-servlet
    mvn install
    cd ..
fi

if [ ! -e target ]
then
    mkdir target
fi

if [ ! -e target/wild* ]
then
    cd target
    unzip $HOME/.m2/repository/org/wildfly/wildfly-dist/8.0.0.Final/wildfly-dist-8.0.0.Final.zip
    cd ..
fi

cp rest-servlet/target/rhq-metric-rest*.war target/wildfly-8.0.0.Final/standalone/deployments/
cp dummy-ui/target/dummy-ui*.war target/wildfly-8.0.0.Final/standalone/deployments/
target/wildfly-8.0.0.Final/bin/standalone.sh -Drhq-metrics.backend=$BACKEND --debug 8787
