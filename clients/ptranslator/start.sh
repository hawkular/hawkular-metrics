#!/bin/sh

# set -x

echo "(Re)building the commons library"
cd ../common
mvn install
if [ $? -ne 0 ]
then
   cd -
   exit 1;
fi
cd -

echo "(Re)building ptrans"
mvn install
if [ $? -ne 0 ]
then
   exit 1;
fi

java -Djava.net.preferIPv4Stack=true -jar target/ptrans-jar-with-dependencies.jar $*
