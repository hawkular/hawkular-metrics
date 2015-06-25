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

# Hawkular-Metrics DockerFile
#
# This dockerfile can be used to create a Hawkular-Metrics docker
# image to be run on Openshift.

FROM ${docker.baseimage}

# The image is maintained by the Hawkular Metrics team
MAINTAINER Hawkular Metrics <hawkular-dev@lists.jboss.org>

# TODO: remove when we have a base image which includes JDK8 support itself
USER root
RUN yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel; \
    yum clean all -y;

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0
USER jboss

#The port that hawkular metrics exposes its endpoint on.
ENV HAWKULAR_METRICS_ENDPOINT_PORT 8080

# Port 8080 is the http endpoint for interacting with Hawkular-Metrics
EXPOSE $HAWKULAR_METRICS_ENDPOINT_PORT

# The Hawkular Metrics version
ENV HAWKULAR_METRICS_VERSION ${project.version}

# Get and copy the hawkular metrics war to the EAP deployment directory
COPY hawkular-metrics-api-jaxrs.war $JBOSS_HOME/standalone/deployments/

# Copy the hawkular kubernetes scripts
ENV HAWKULAR_METRICS_SCRIPT_DIRECTORY /opt/hawkular/scripts/
COPY hawkular-metrics-poststart.sh $HAWKULAR_METRICS_SCRIPT_DIRECTORY
COPY hawkular-metrics-liveness.sh $HAWKULAR_METRICS_SCRIPT_DIRECTORY

# Overwrite the welcome-content to display a more appropriate status page
COPY welcome-content $JBOSS_HOME/welcome-content/

# Change the permissions so that the user running the image can start up Hawkular Metrics
# TODO: we can probably remove this once we get a new base image designed to work with OpenShift v3 1.0.0
USER root
RUN chmod -R 777 /opt

USER jboss

CMD $JBOSS_HOME/bin/standalone.sh -b 0.0.0.0 -bmanagement 0.0.0.0 -Dhawkular-metrics.cassandra-nodes=hawkular-cassandra
