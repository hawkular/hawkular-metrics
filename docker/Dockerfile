#Dockerfile for rhq-metrics

FROM jboss/base-jdk:7

MAINTAINER Armine Hovsepyan <ahovsepy@redhat.com>, Garik Khachikyan <gkhachik@redhat.com>

USER root

RUN yum -y install @"Development Tools" wget git tar unzip npm nodejs

RUN wget --quiet http://download.nextag.com/apache/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz -O /opt/maven.tar.gz && tar xzf /opt/maven.tar.gz -C /opt

# root is not recommended for mvn builds.
USER jboss

ENV  M2_HOME /opt/apache-maven-3.2.3 
ENV PATH $M2_HOME/bin:$PATH 
ENV HOME /opt/jboss

#donwload rhq-metrics from github
RUN git clone https://github.com/rhq-project/rhq-metrics.git

# Get all maven libs to make process of build really faster
RUN cd /opt/jboss/rhq-metrics; \
echo -e "\n** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **\nLatest git hash is: [$(git log -n 1 --pretty=format:"%H")]\n** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **\n"; \
mvn install -e -X -DskipTests

# pull latest sources, build & start rhq-metrics
ENTRYPOINT  cd /opt/jboss/rhq-metrics ; \
git clean -xdf; \
git pull; \
./start.sh 0.0.0.0;

CMD []