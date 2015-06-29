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

# Hawkular-Metrics Cassandra Docker Image

# Image based off of the jboss base image with JDK8
FROM ${docker.baseimage}

# The image is maintained by the Hawkular Metrics team
MAINTAINER Hawkular Metrics <hawkular-dev@lists.jboss.org>

# Cassandra CQL transport port
EXPOSE 9042
# Cassandra Thirft transport port
EXPOSE 9160
# Cassandra TCP port
EXPOSE 7000
# Cassandra SSL port
EXPOSE 7001

# The Cassandra version
ENV CASSANDRA_VERSION ${cassandra.version}

# The Cassandra home location
ENV CASSANDRA_HOME /opt/apache-cassandra

# TODO: figure out if there is a better way to handle this
# If we don't specify values here then the cassandra bin tries to automatically determine
# the size which relies on other binaries not installed in the RHEL image.
ENV MAX_HEAP_SIZE=512M
ENV HEAP_NEWSIZE=100M

# Become the root user to be able to install and setup Cassandra under /opt
USER root

# Copy the Cassandra binary to the /opt directory
RUN cd /opt; \
    curl -LO https://archive.apache.org/dist/cassandra/$CASSANDRA_VERSION/apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz && \
    tar xzf apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz && \
    rm apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz && \
    ln -s apache-cassandra-$CASSANDRA_VERSION apache-cassandra

# Copy our version of the cassandra configuration file over to the filesystem
COPY cassandra.yaml /opt/apache-cassandra/conf/cassandra.yaml

# Copy the jar containing the kubernetes away jar
COPY cassandra-seed-provider.jar /opt/apache-cassandra/lib/

# Copy our customized run script over to the cassandra bin directory
COPY cassandra-docker.sh /opt/apache-cassandra/bin/

# Copy the preStop script over to the cassandra bin directory
COPY cassandra-docker-pre-stop.sh /opt/apache-cassandra/bin/

# Copy the Readiness script over to the cassandra bin directory
COPY cassandra-docker-ready.sh /opt/apache-cassandra/bin/

# TODO: remove this line once https://bugzilla.redhat.com/show_bug.cgi?id=1201848 has been fixed
ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Add the Cassandra bin directory to $PATH
ENV PATH /opt/apache-cassandra/bin:$PATH

# The name of the service exposing the Cassandra nodes
ENV CASSANDRA_NODES_SERVICE_NAME hawkular-cassandra-nodes

# Create a 'Cassandra' user to own the directory and set it to be readable & writable by any user
RUN groupadd -r cassandra -g 312 && \
    useradd -u 313 -r -g cassandra -d /opt/apache-cassandra -s /sbin/nologin cassandra && \
    chown -R cassandra:cassandra /opt/apache-cassandra && \
    chmod -R go+rw /opt/apache-cassandra

USER cassandra

CMD ["/opt/apache-cassandra/bin/cassandra-docker.sh", "--seed_provider_classname=org.hawkular.openshift.cassandra.OpenshiftSeedProvider" ,"--cluster_name=hawkular-metrics"]
