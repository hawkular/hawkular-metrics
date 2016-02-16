<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
    and other contributors as indicated by the @author tags.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xalan="http://xml.apache.org/xalan"
                xmlns:ds="urn:jboss:domain:datasources:3.0" xmlns:ra="urn:jboss:domain:resource-adapters:3.0"
                xmlns:ejb3="urn:jboss:domain:ejb3:3.0"
                xmlns:logging="urn:jboss:domain:logging:3.0" xmlns:undertow="urn:jboss:domain:undertow:2.0"
                xmlns:tx="urn:jboss:domain:transactions:3.0"
                version="2.0" exclude-result-prefixes="xalan ds ra ejb3 logging undertow tx">

  <!-- will indicate if this is a "dev" build or "production" build -->
  <xsl:param name="kettle.build.type"/>
  <xsl:param name="uuid.hawkular.accounts.backend"/>

  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" xalan:indent-amount="4" standalone="no"/>
  <xsl:strip-space elements="*"/>

  <!-- //*[local-name()='secure-deployment'] is an xPath's 1.0 way of saying of xPath's 2.0 prefix-less selector //*:secure-deployment  -->
  <xsl:template match="//*[*[local-name()='secure-deployment']]">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
      <secure-deployment name="hawkular-metrics-component.war">
        <realm>hawkular</realm>
        <resource>hawkular-accounts-backend</resource>
        <use-resource-role-mappings>true</use-resource-role-mappings>
        <enable-cors>true</enable-cors>
        <enable-basic-auth>true</enable-basic-auth>
        <!-- copy the secret value from the previous available secure-deployment -->
        <credential name="secret">
          <xsl:value-of
              select="*[local-name()='secure-deployment']/*[local-name()='credential' and @name='secret']/text()"/>
        </credential>
      </secure-deployment>
    </xsl:copy>
  </xsl:template>

  <xsl:template match="//*[local-name()='subsystem']/*[local-name()='server' and @name='default']">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
      <!-- Topics and queues must be also exposed under jboss/exported to be visible to external clients -->
      <jms-topic name="HawkularAvailData"
                 entries="java:/topic/HawkularAvailData java:jboss/exported/topic/HawkularAvailData"/>
      <jms-topic name="HawkularMetricData"
                 entries="java:/topic/HawkularMetricData java:jboss/exported/topic/HawkularMetricData"/>
      <jms-queue name="hawkular/metrics/gauges/new"
                 entries="java:/queue/hawkular/metrics/gauges/new java:jboss/exported/queue/hawkular/metrics/gauges/new"/>
      <jms-queue name="hawkular/metrics/counters/new"
                 entries="java:/queue/hawkular/metrics/counters/new java:jboss/exported/queue/hawkular/metrics/counters/new"/>
      <jms-queue name="hawkular/metrics/availability/new"
                 entries="java:/queue/hawkular/metrics/availability/new java:jboss/exported/queue/hawkular/metrics/availability/new"/>
    </xsl:copy>
  </xsl:template>

  <!-- copy everything else as-is -->
  <xsl:template match="node()|@*">
    <xsl:copy>
      <xsl:apply-templates select="node()|@*"/>
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
