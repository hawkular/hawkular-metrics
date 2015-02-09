<!--

    Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
<!--
 Taken from https://github.com/pilhuhn/swagger-core/blob/org.rhq.helpers.rest_docs_generator.test/modules/java-jaxrs-org.rhq.helpers.rest_docs_generator.test/src/main/xsl/apiout2html.xsl
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <xsl:param name="basePath"/>
  <xsl:strip-space elements="a"/>

  <xsl:template match="/api">
    <html>
      <head>
        <title>Hawkular Metrics REST-Api overview</title>
        <style type="text/css">
          h2 {background-color:#ADD8E6   }
          h3 {background-color:#C0C0C0   }
          th {font-weight:bold; font-size:120% }
          em {font-style:italic}
          .exp {background-color:#CC6666 }
        </style>
      </head>
      <body>
        <h1>REST-api documentation</h1>
        <em>Base path (if not otherwise specified) : <xsl:value-of select="$basePath"/></em>
        <h2>Table of contents</h2>
        <ul>
          <xsl:for-each select="class">
            <xsl:sort select="@basePath"/>
            <xsl:sort select="@path"/>
            <li>
              <xsl:element name="a">
                <xsl:attribute name="href">#<xsl:value-of select="@path"/></xsl:attribute>
                <xsl:call-template name="class-level-path"/>
              </xsl:element>
              <xsl:text> </xsl:text>
              <xsl:value-of select="@shortDesc"/>
            </li>
          </xsl:for-each>
        </ul>
        <xsl:apply-templates>
          <xsl:sort select="@basePath"/>
          <xsl:sort select="@path"/>
        </xsl:apply-templates>
      </body>
    </html>
  </xsl:template>

  <xsl:template name="class-level-path">
    <xsl:choose>
      <xsl:when test="@basePath">
        <xsl:value-of select="@basePath"/>
        <xsl:if test="not(substring(@basePath,string-length(@basePath)-1)='/')">/</xsl:if>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$basePath"/>
        <xsl:if test="not(substring($basePath,string-length($basePath)-1)='/')">/</xsl:if>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:value-of select="@path"/>
  </xsl:template>

  <xsl:template match="class">
    <xsl:element name="h2">
      <xsl:attribute name="id"><xsl:value-of select="@path"/></xsl:attribute>
      <xsl:if test="contains(@shortDesc,'EXPERIMENT')">
        <xsl:attribute name="class">exp</xsl:attribute>
      </xsl:if>
      <!--/<xsl:value-of select="@path"/>-->
      <xsl:call-template name="class-level-path"/>
      <xsl:if test="@shortDesc">
      : <xsl:value-of select="@shortDesc"/>
      </xsl:if>
    </xsl:element>
    <em><xsl:value-of select="@description"/></em>
    <br/>
    <xsl:if test="method">
      <xsl:apply-templates>
        <xsl:sort select="@path"/>
      </xsl:apply-templates>
    </xsl:if>
    <p/>
  </xsl:template>

  <xsl:template match="method">
    <h3><xsl:value-of select="@method"/><xsl:text xml:space="preserve"> /</xsl:text><xsl:value-of select="../@path"/>
      <xsl:if test="not(@path = '')">/</xsl:if><xsl:value-of select="@path"/>
    </h3>
    <em><xsl:value-of select="@description"/></em>
    <br/>

  </xsl:template>

  <xsl:template match="param">
  </xsl:template>

  <xsl:template match="error">
  </xsl:template>

  <xsl:template match="produces">
  </xsl:template>

</xsl:stylesheet>
