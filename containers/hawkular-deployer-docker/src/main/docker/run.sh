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

set -ex

# Set the default values here in case someone is running the script locally and not in a container
image_prefix=${IMAGE_PREFIX:-sosiouxme/}
image_version=${IMAGE_VERSION:-latest}
master_url=${MASTER_URL:-https://kubernetes.default.svc.cluster.local:8443}

project=${PROJECT:-default}

# the master certificate and service account tokens
master_ca=${MASTER_CA:-/var/run/secrets/kubernetes.io/serviceaccount/ca.crt}
token_file=${TOKEN_FILE:-/var/run/secrets/kubernetes.io/serviceaccount/token}

# directory to perform all the processing
dir=${PROCESSING_DIR:-_output} #directory used to write files which generating certificates

hawkular_metrics_hostname=${HAWKULAR_METRICS_HOSTNAME:-hawkular-metrics.example.com}
hawkular_metrics_alias=${HAWKULAR_METRICS_ALIAS:-hawkular-metrics}

hawkular_cassandra_alias=${HAWKULAR_CASSANDRA_ALIAS:-hawkular-cassandra}

rm -rf $dir && mkdir -p $dir && chmod 700 $dir || :

# cp/generate CA
if [ -s /secret/ca.key ]; then
  cp {/secret,$dir}/ca.key
  cp {/secret,$dir}/ca.crt
  echo "01" > $dir/ca.serial.txt
else
    openshift admin ca create-signer-cert  \
      --key="${dir}/ca.key" \
      --cert="${dir}/ca.crt" \
      --serial="${dir}/ca.serial.txt" \
      --name="metrics-signer"
fi

# Use existing or generate new Hawkular Metrics certificates
if [ -n "${HAWKUKAR_METRICS_PEM}" ]; then
    echo "${HAWKULAR_METRICS_PEM}" | base64 -d > $dir/hawkular-metrics.pem
elif [ -s /secret/hawkular-metrics.pem ]; then
    # use files from secret if present
    cp {/secret,$dir}/hawkular-metrics.pem $dir
else #fallback to creating one
    openshift admin ca create-server-cert  \
      --key=$dir/hawkular-metrics.key \
      --cert=$dir/hawkular-metrics.crt \
      --hostnames=hawkular-metrics,${hawkular_metrics_hostname} \
      --signer-cert="$dir/ca.crt" --signer-key="$dir/ca.key" --signer-serial="$dir/ca.serial.txt"
      cat $dir/hawkular-metrics.key $dir/hawkular-metrics.crt > $dir/hawkular-metrics.pem
fi

# Use existing or generate new Hawkular Cassandra certificates
if [ -n "${HAWKUKAR_CASSANDRA_PEM}" ]; then
    echo "${HAWKULAR_CASSANDRA_PEM}" | base64 -d > $dir/hawkular-cassandra.pem
elif [ -s /secret/hawkular-cassandra.pem ]; then
    # use files from secret if present
    cp {/secret,$dir}/hawkular-cassandra.pem $dir
else #fallback to creating one
    openshift admin ca create-server-cert  \
      --key=$dir/hawkular-cassandra.key \
      --cert=$dir/hawkular-cassandra.crt \
      --hostnames=hawkular-cassandra \
      --signer-cert="$dir/ca.crt" --signer-key="$dir/ca.key" --signer-serial="$dir/ca.serial.txt"
      cat $dir/hawkular-cassandra.key $dir/hawkular-cassandra.crt > $dir/hawkular-cassandra.pem
fi

echo 03 > $dir/ca.serial.txt  # otherwise openssl chokes on the file

# Convert the *.pem files into java keystores
echo "Generating randomized passwords for the Hawkular Metrics and Cassandra keystores and truststores"
hawkular_metrics_keystore_password=`cat /dev/urandom | tr -dc _A-Z-a-z-0-9 | head -c15`
hawkular_metrics_truststore_password=`cat /dev/urandom | tr -dc _A-Z-a-z-0-9 | head -c15`
hawkular_cassandra_keystore_password=`cat /dev/urandom | tr -dc _A-Z-a-z-0-9 | head -c15`
hawkular_cassandra_truststore_password=`cat /dev/urandom | tr -dc _A-Z-a-z-0-9 | head -c15`

echo "Hawkular Metrics Keystore Password :" $hawkular_metrics_keystore_password
echo "Hawkular Metrics Keystore Password :" $hawkular_metrics_truststore_password
echo "Hawkular Cassandra Keystore Password :" $hawkular_cassandra_keystore_password
echo "Hawkular Cassandra Keystore Password :" $hawkular_cassandra_truststore_password

echo "Creating the Hawkular Metrics keystore from the PEM file"
openssl pkcs12 -export -in $dir/hawkular-metrics.pem -out $dir/hawkular-metrics.pkcs12 -name $hawkular_metrics_alias -noiter -nomaciter -password pass:$hawkular_metrics_keystore_password
keytool -v -importkeystore -srckeystore $dir/hawkular-metrics.pkcs12 -srcstoretype PKCS12 -destkeystore $dir/hawkular-metrics.keystore -deststoretype JKS -deststorepass $hawkular_metrics_keystore_password -srcstorepass $hawkular_metrics_keystore_password

echo "Creating the Hawkular Cassandra keystore from the PEM file"
openssl pkcs12 -export -in $dir/hawkular-cassandra.pem -out $dir/hawkular-cassandra.pkcs12 -name $hawkular_cassandra_alias -noiter -nomaciter -password pass:$hawkular_cassandra_keystore_password
keytool -v -importkeystore -srckeystore $dir/hawkular-cassandra.pkcs12 -srcstoretype PKCS12 -destkeystore $dir/hawkular-cassandra.keystore -deststoretype JKS -deststorepass $hawkular_cassandra_keystore_password -srcstorepass $hawkular_cassandra_keystore_password

echo "Creating the Hawkular Metrics Certificate"
keytool -noprompt -export -alias $hawkular_metrics_alias -file $dir/hawkular-metrics.cert -keystore $dir/hawkular-metrics.keystore -storepass $hawkular_metrics_keystore_password

echo "Creating the Hawkular Cassandra Certificate"
keytool -noprompt -export -alias $hawkular_cassandra_alias -file $dir/hawkular-cassandra.cert -keystore $dir/hawkular-cassandra.keystore -storepass $hawkular_cassandra_keystore_password

echo "Importing the Hawkular Metrics Certificate into the Cassandra Truststore"
keytool -noprompt -import -v -trustcacerts -alias $hawkular_metrics_alias -file $dir/hawkular-metrics.cert -keystore $dir/hawkular-cassandra.truststore -trustcacerts -storepass $hawkular_cassandra_truststore_password

echo "Importing the Hawkular Cassandra Certificate into the Hawkular Metrics Truststore"
keytool -noprompt -import -v -trustcacerts -alias $hawkular_cassandra_alias -file $dir/hawkular-cassandra.cert -keystore $dir/hawkular-metrics.truststore -trustcacerts -storepass $hawkular_metrics_truststore_password

echo "Importing the Hawkular Cassandra Certificate into the Cassandra Truststore"
keytool -noprompt -import -v -trustcacerts -alias $hawkular_cassandra_alias -file $dir/hawkular-cassandra.cert -keystore $dir/hawkular-cassandra.truststore -trustcacerts -storepass $hawkular_cassandra_truststore_password

echo
echo "Creating the Hawkular Metrics Secrets configuration json file"
cat > $dir/hawkular-metrics-secrets.json <<EOF
    {
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata":
      { "name": "hawkular-metrics-secrets",
        "labels": {
          "metrics-infra": "hawkular-metrics"
        }
      },
      "data":
      {
        "hawkular-metrics.keystore": "$(base64 -w 0 $dir/hawkular-metrics.keystore)",
        "hawkular-metrics.keystore.password": "$(base64 <<< `echo $hawkular_metrics_keystore_password`)",
        "hawkular-metrics.truststore": "$(base64 -w 0 $dir/hawkular-metrics.truststore)",
        "hawkular-metrics.truststore.password": "$(base64 <<< `echo $hawkular_metrics_truststore_password`)",
        "hawkular-metrics.keystore.alias": "$(base64 <<< `echo $hawkular_metrics_alias`)"
      }
    }
EOF

echo
echo "Creating the Hawkular Metrics Certificate Secrets configuration json file"
cat > $dir/hawkular-metrics-certificate.json <<EOF
    {
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata":
      { "name": "hawkular-metrics-certificate",
        "labels": {
          "metrics-infra": "hawkular-metrics"
        }
      },
      "data":
      {
        "hawkular-metrics.certificate": "$(base64 -w 0 $dir/hawkular-metrics.cert)"
      }
    }
EOF


echo
echo "Creating the Cassandra Secrets configuration file"
cat > $dir/cassandra-secrets.json <<EOF
    {
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata":
      { "name": "hawkular-cassandra-secrets",
        "labels": {
          "metrics-infra": "hawkular-cassandra"
        }
      },
      "data":
      {
        "cassandra.keystore": "$(base64 -w 0 $dir/hawkular-cassandra.keystore)",
        "cassandra.keystore.password": "$(base64 <<< `echo $hawkular_cassandra_keystore_password`)",
        "cassandra.keystore.alias": "$(base64 <<< `echo $hawkular_cassandra_alias`)",
        "cassandra.truststore": "$(base64 -w 0 $dir/hawkular-cassandra.truststore)",
        "cassandra.truststore.password": "$(base64 <<< `echo $hawkular_cassandra_truststore_password`)"
      }
    }
EOF

echo
echo "Creating the Cassandra Certificate Secrets configuration json file"
cat > $dir/cassandra-certificate.json <<EOF
    {
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata":
      { "name": "hawkular-cassandra-certificate",
        "labels": {
          "metrics-infra": "hawkular-cassandra"
        }
      },
      "data":
      {
        "cassandra.certificate": "$(base64 -w 0 $dir/hawkular-cassandra.cert)"
      }
    }
EOF

# set up configuration for client
if [ -n "${WRITE_KUBECONFIG}" ]; then
    # craft a kubeconfig, usually at $KUBECONFIG location
    oc config set-cluster master \
      --api-version='v1' \
      --certificate-authority="${master_ca}" \
      --server="${master_url}"
    oc config set-credentials account \
      --token="$(cat ${token_file})"
    oc config set-context current \
      --cluster=master \
      --user=account \
      --namespace="${PROJECT}"
    oc config use-context current
fi

echo "Deleting any previous deployment"
oc delete all --selector="metrics-infra=hawkular-metrics"
oc delete all --selector="metrics-infra=hawkular-cassandra"
oc delete all --selector="metrics-infra=heapster"
oc delete all --selector="metrics-infra=support"

# deleting the exisiting service account
oc delete sa --selector="metrics-infra=support"

# delete the templates
oc delete templates --selector="metrics-infra=hawkular-metrics"
oc delete templates --selector="metrics-infra=hawkular-cassandra"
oc delete templates --selector="metrics-infra=heapster"
oc delete templates --selector="metrics-infra=support"

# delete the secrets
oc delete secrets --selector="metrics-infra=hawkular-metrics"
oc delete secrets --selector="metrics-infra=hawkular-cassandra"
oc delete secrets --selector="metrics-infra=heapster"
oc delete secrets --selector="metrics-infra=support"

echo "Creating secrets"
oc create -f $dir/hawkular-metrics-secrets.json
oc create -f $dir/hawkular-metrics-certificate.json
oc create -f $dir/cassandra-secrets.json
oc create -f $dir/cassandra-certificate.json

echo "Creating templates"
oc create -f templates/hawkular-metrics.json
oc create -f templates/hawkular-cassandra.json
oc create -f templates/heapster.json
oc create -f templates/support.json

echo "Enabling service account"
sa="system:serviceaccount:$project:hawkular"
oadm policy add-cluster-role-to-user cluster-reader $sa

echo "Deploying components"
oc process hawkular-metrics-template | oc create -f -
oc process hawkular-cassandra-template | oc create -f -
oc process hawkular-heapster-template | oc create -f -
oc process hawkular-support-template | oc create -f -

echo 'Success!'
