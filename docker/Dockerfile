#Dockerfile for rhq-metrics

FROM vnguyen/wildfly-jdk8

MAINTAINER Armine Hovsepyan <ahovsepy@redhat.com>, Garik Khachikyan <gkhachik@redhat.com>, Viet Nguyen <vnguyen@redhat.com>
RUN /opt/jboss/wildfly/bin/add-user.sh rhqmetrics rhqmetrcis --silent
COPY rhq-metric-rest*.war /opt/jboss/wildfly/standalone/deployments/
COPY metrics-console-*.war /opt/jboss/wildfly/standalone/deployments/
COPY explorer-*.war  /opt/jboss/wildfly/standalone/deployments/

CMD ["/opt/jboss/wildfly/bin/standalone.sh","-b","0.0.0.0","-bmanagement","0.0.0.0","-Drhq-metrics.backend","cass"]
