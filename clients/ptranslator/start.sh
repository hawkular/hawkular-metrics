#!/bin/sh

echo "(Re)building the commons library"
cd ../common
mvn install
cd -

echo "(Re)building ptrans"
mvn install

java -Djava.net.preferIPv4Stack=true -jar target/ptrans-jar-with-dependencies.jar $*
