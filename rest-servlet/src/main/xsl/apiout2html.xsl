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
        <title>Hawkular Metrics REST-Api documentation</title>
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
    <p/>
    Defining class: <xsl:value-of select="@name"/><br/>
    <br/>
    <xsl:call-template name="print-created-media-types">
      <xsl:with-param name="produces" select="produces"/>
    </xsl:call-template>
    <xsl:if test="method">
      Methods:<br/>
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
    <xsl:if test="notes">
      <h4>Notes</h4>
      <xsl:choose>
        <xsl:when test="notes/xml">
          <!-- We should perhaps not copy them literally, but do some translation from docbook to html -->
          <xsl:copy-of select="notes/xml/*"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="notes"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:if>

    <xsl:if test="not(@gzip = '')">
      <p><em>Supports returning a gzip'ed Content-Encoding</em></p>
    </xsl:if>
    <xsl:choose>
    <xsl:when test="param">
    Parameters:
    <table>
        <tr><th>Name</th><th>P.Type</th><th>Description</th><th>Required</th><th>Type</th><th>Allowed values</th><th>Default value</th></tr>
      <xsl:apply-templates select="param"/>
    </table>
    </xsl:when>
      <xsl:otherwise>
        This method has no parameters
      </xsl:otherwise>
    </xsl:choose>
    <br/>
    Return type: <xsl:value-of select="@returnType"/>
    <p/>
    <xsl:if test="error">
      Error codes:<br/>
      <table>
          <tr>
            <th>Code</th><th>Reason</th>
          </tr>
        <xsl:apply-templates select="error"/>
      </table>
    </xsl:if>
  </xsl:template>

  <xsl:template match="param">
    <tr>
      <td><xsl:value-of select="@name"/></td>
      <td><xsl:value-of select="@paramType"/></td>
      <td><xsl:value-of select="@description"/></td>
      <td><xsl:value-of select="@required"/></td>
      <td><xsl:value-of select="@type"/></td>
      <td><xsl:value-of select="@allowableValues"/></td>
      <td><xsl:value-of select="@defaultValue"/></td>
    </tr>
  </xsl:template>

  <xsl:template match="error">
    <tr>
        <td><xsl:value-of select="@code"/></td>
        <td><xsl:value-of select="@reason"/></td>
    </tr>
  </xsl:template>

  <!-- emit media types produced -->
  <xsl:template name="print-created-media-types">
    <xsl:param name="produces"/>

    <xsl:if test="$produces">
      <b>Produces:</b>
      <ul>
      <xsl:for-each select="$produces/type">
        <li>
        <xsl:value-of select="."/>
        </li>
      </xsl:for-each>
      </ul>
  </xsl:if>

  </xsl:template>

</xsl:stylesheet>
