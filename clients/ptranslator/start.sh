#!/bin/sh
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

java -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=file:log4j-dev.xml -jar target/ptrans-all.jar $*
