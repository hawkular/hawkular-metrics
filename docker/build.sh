#!/bin/bash
pushd ..
mvn install -DskipTests
popd
mv ../rest-servlet/target/rhq-metric-rest*.war .
mv ../ui/console/target/metrics-console-*.war .
mv ../ui/explorer/target/explorer-*.war .
docker build --rm --tag rhq-metrics:latest . 
