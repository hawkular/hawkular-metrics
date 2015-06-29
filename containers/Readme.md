Building the Docker Images
==========================

To build the docker images, you will need to have access to a machine running docker which has been configured to have its remote API exposed.

You will need to expose this host by using the environment variable DOCKER_HOST or by specifing the runtime property -Ddocker.host

You will also need to enable the docker-build maven profile.

Example, assuming your docker host is running on your local machine, using the tcp protocol and listening on port 2375:

export DOCKER_HOST=tcp://127.0.0.1:2375
mvn clean install -Pdocker-build

or:

mvn clean install -Pdocker-build -Ddocker.host=tcp://127.0.0.1:2375


Running the Container Tests
===========================

The container tests are meant to be run on OpenShift v3 1.0.0

# Download OpenShift and extract it
wget https://github.com/openshift/origin/releases/download/v1.0.0/openshift-origin-v1.0.0-67617dd-linux-amd64.tar.gz
tar zxvf openshift-origin-v1.0.0-67617dd-linux-amd64.tar.gz

# Expose the OpenShift binaries to $PATH
export PATH=`pwd`:$PATH

# set the address you want openshift to be exposed at
# NOTE: you cannot use 127.0.0.1 or localhost here as it needs to be an address which is resolvable from within docker containers
export BINDING_ADDRESS=INSERT-YOUR-BINDING-ADDRESS-HERE

# create the configuration files you wish to use
./openshift start --write-config=openshift.local.config --hostname=$BINDING_ADDRESS --public-master=$BINDING_ADDRESS

# Temporary fix for heapster: for 1.0.0 heapster will not function properly due to it requring an non-secured /stats endpoint
# you will need to configure OpenShift to expose the RO endpoint as being unsecured

edit the openshift.local.config/node-$BINDING_ADDRESS/node-config.yaml to add the following at the end:

kubeletArguments:
  read-only-port:
  - "10266"

Starting OpenShift
------------------

NOTE: you will need to start dokcer as root.

openshift start --master-config=openshift.local.config/master/master-config.yaml --node-config=openshift.local.config/node-$BINDING_ADDRESS/node-config.yaml

NOTE: make sure that the logs say that the DNS server has been started properly
If you see something in the logs like "Could not start DNS: listen tcp 0.0.0.0:53: bind: address already in use" you will need to stop OpenShift and resolve the issue before continuing.
You should see something like this instead: "OpenShift DNS listening at 0.0.0.0:53"

Checking that OpenShift is up and running:
Open a browser and go to: https://$BINDING_ADDRESS:8443
Note: you may need to explicitly enter the https here, otherwise you might get nonsense bytes returned back

Note: OpenShift is using self signed certificates, so it there will be a warning in your browser about this.

By default, OpenShift will allow for any username and any password to login. Please see the OpenShift documentation on how to specify a different login mechanism.

By default, the user who logs in will not have any permissions. To give your 'admin' user cluster admin priviledges, you can use the following command:

oadm --config=`pwd`/openshift.local.config/master/admin.kubeconfig policy add-cluster-role-to-user cluster-admin admin

Running the Tests
-----------------
To run the tests, you will need to specify the following environment variables:

export OPENSHIFT_HOME=<DIRECTORY WERE YOU INSTALLED OPENSHIFT>
export KUBERNETES_TRUST_CERT=true
export KUBERNETES_MASTER=http://$BINDING_ADDRESS:8443
export KUBERNETES_CLIENT_CERTIFICATE_FILE=$OPENSHIFT_HOME/openshift.local.config/master/admin.crt
export KUBERNETES_CLIENT_KEY_FILE=$OPENSHIFT_HOME/openshift.local.config/master/admin.key
mvn clean install -Pcontainer-tests

