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

# Set up the parameters to pass to EAP while removing the ones specific to the wrapper
eap_args=

for args in "$@"
do
  if [[ $args == --hmw\.* ]]; then
    case $args in
      --hmw.keystore=*)
        KEYSTORE="${args#*=}"
        ;;
      --hmw.truststore=*)
        TRUSTSTORE="${args#*=}"
        ;;
      --hmw.keystore_password=*)
        KEYSTORE_PASSWORD="${args#*=}"
        ;;
      --hmw.keystore_password_file=*)
        KEYSTORE_PASSWORD_FILE="${args#*=}"
        ;;
      --hmw.truststore_password=*)
        TRUSTSTORE_PASSWORD="${args#*=}"
        ;;
      --hmw.truststore_password_file=*)
        TRUSTSTORE_PASSWORD_FILE="${args#*=}"
        ;;
    esac
  else
    eap_args="$eap_args $args"
  fi
done

if [ -n "$KEYSTORE_PASSWORD_FILE" ]; then
   KEYSTORE_PASSWORD=$(cat $KEYSTORE_PASSWORD_FILE)
fi

if [ -n "$TRUSTSTORE_PASSWORD_FILE" ]; then
   TRUSTSTORE_PASSWORD=$(cat $TRUSTSTORE_PASSWORD_FILE)
fi

# Setup the truststore so that it will accept the OpenShift cert
HAWKULAR_METRICS_AUTH_DIR=$HAWKULAR_METRICS_DIRECTORY/auth
mkdir $HAWKULAR_METRICS_AUTH_DIR
pushd $HAWKULAR_METRICS_AUTH_DIR

cp $KEYSTORE hawkular-metrics.keystore
cp $TRUSTSTORE hawkular-metrics.truststore

chmod a+rw hawkular-metrics.*

KEYTOOL_COMMAND=/usr/lib/jvm/java-1.8.0/jre/bin/keytool
$KEYTOOL_COMMAND -noprompt -import -v -trustcacerts -alias kubernetes-master -file /var/run/secrets/kubernetes.io/serviceaccount/ca.crt -keystore hawkular-metrics.truststore -trustcacerts -storepass $TRUSTSTORE_PASSWORD

popd


exec /opt/eap/bin/standalone.sh \
  -Djavax.net.ssl.keyStore=$HAWKULAR_METRICS_AUTH_DIR/hawkular-metrics.keystore \
  -Djavax.net.ssl.keyStorePassword=$KEYSTORE_PASSWORD \
  -Djavax.net.ssl.trustStore=$HAWKULAR_METRICS_AUTH_DIR/hawkular-metrics.truststore \
  -Djavax.net.ssl.trustStorePassword=$TRUSTSTORE_PASSWORD \
  $eap_args
