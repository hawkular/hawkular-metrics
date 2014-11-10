#!/bin/sh

echo "Checking project version..."
VERSION=`mvn  help:evaluate -Dexpression=project.version | grep -vF "[INFO]"`
echo "Project version is ${VERSION}"

echo "(Re)building the commons library"
cd ../common
mvn install
cd -

echo "(Re)building ptrans"
mvn install

java -Djava.net.preferIPv4Stack=true -jar target/ptrans-${VERSION}-jar-with-dependencies.jar $*
