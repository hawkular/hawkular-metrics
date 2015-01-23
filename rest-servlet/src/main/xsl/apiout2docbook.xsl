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
 and modified for docbook
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns="http://docbook.org/ns/docbook"
    xml:lang="en">

  <xsl:param name="basePath"/>
  <xsl:output xml:lang="en"/>

  <xsl:template match="/api">
    <xsl:element name="section">
      <xsl:attribute name="version">5.0</xsl:attribute>
      <!--<xsl:attribute name="xmlns">http://docbook.org/ns/docbook</xsl:attribute>-->
      <title>REST Api documentation</title>
      <subtitle>Base path (if not otherwise specified) : <xsl:value-of select="$basePath"/></subtitle>
      <toc>
        <xsl:for-each select="class">
          <xsl:sort select="@basePath"/>
          <xsl:sort select="@path"/>
          <xsl:element name="tocentry">
            <link>
            <xsl:attribute name="linkend">
              <xsl:value-of select="generate-id(@path)"/>
            </xsl:attribute>
            <xsl:if test="@basePath">
              <xsl:value-of select="@basePath"/>
            </xsl:if>
            <xsl:value-of select="@path"/>
            </link>
          </xsl:element>
        </xsl:for-each>
      </toc>
      <xsl:apply-templates select="class">
        <xsl:sort select="@basePath"/>
        <xsl:sort select="@path"/>
      </xsl:apply-templates>
      <xsl:apply-templates select="data">
        <xsl:sort select="@name"/>
      </xsl:apply-templates>
    </xsl:element>
  </xsl:template>

  <xsl:template match="class">
    <xsl:element name="section">
      <xsl:attribute name="xml:id">
          <xsl:value-of select="generate-id(@path)"/>
      </xsl:attribute>
      <title>
        <!--/<xsl:value-of select="@path"/>-->
        <xsl:call-template name="class-level-path"/>
        <xsl:if test="@shortDesc">
        : <xsl:value-of select="@shortDesc"/>
        </xsl:if>
      </title>
      <subtitle><xsl:value-of select="@description"/></subtitle>
      <simpara>
        Defining class:
        <xsl:value-of select="@name"/>
      </simpara>
      <xsl:call-template name="print-created-media-types">
        <xsl:with-param name="produces" select="produces"/>
      </xsl:call-template>
      <para>
        <xsl:if test="method">
          <itemizedlist>
            <title>Methods</title>
            <xsl:apply-templates/>
          </itemizedlist>
        </xsl:if>
      </para>
    </xsl:element>

  </xsl:template>

  <xsl:template match="method">
    <listitem>
      <simpara>
        <emphasis role="bold"><xsl:value-of select="@method"/><xsl:text xml:space="preserve"> </xsl:text><xsl:if
          test="not(../@path = '/')"><xsl:value-of
          select="../@path"/></xsl:if><xsl:if test="not(@path = '')">/</xsl:if><xsl:value-of select="@path"/>
        </emphasis>
      </simpara>
      <formalpara>
        <title>Description</title>
        <xsl:value-of select="@description"/></formalpara>
      <xsl:if test="notes">
        <formalpara>
          <title>Notes</title>
          <para>
            <xsl:choose>
              <xsl:when test="notes/xml">
                <xsl:copy-of select="notes/xml/*"/>
              </xsl:when>
              <xsl:otherwise>
                <xsl:value-of select="notes"/>
              </xsl:otherwise>
            </xsl:choose>
          </para>
        </formalpara>
      </xsl:if>
      <xsl:if test="@gzip = 'true'">
        <simpara>Supports returning a gzip'ed Content-Encoding</simpara>
      </xsl:if>
      <xsl:choose>
        <xsl:when test="param">
          <table>
            <title>Parameters:</title>
            <tr>
              <th>Name</th>
              <th>P.Type</th>
              <th>Description</th>
              <th>Required</th>
              <th>Type</th>
              <th>Allowed values</th>
              <th>Default value</th>
            </tr>
            <xsl:apply-templates select="param"/>
          </table>
        </xsl:when>
        <xsl:otherwise>
          <simpara>
            This method has no parameters
          </simpara>
        </xsl:otherwise>
      </xsl:choose>
      <simpara>
        Return type:
        <xsl:choose>
        <xsl:when test="starts-with(@returnTypeId,'...')">
          <link>
            <xsl:attribute name="linkend">
                <xsl:value-of select="@returnTypeId"/>
            </xsl:attribute>
            <xsl:value-of select="@returnType"/>
          </link>
        </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="@returnType"/>
          </xsl:otherwise>
        </xsl:choose>
      </simpara>
      <xsl:if test="error">
        <table>
          <title>Error codes:</title>
          <tr>
              <th>Code</th>
              <th>Reason</th>
            </tr>
          <xsl:apply-templates select="error"/>
        </table>
      </xsl:if>
    </listitem>
  </xsl:template>

  <xsl:template match="param">
    <tr>
      <td>
        <xsl:choose>
          <xsl:when test="@name">
          <xsl:value-of select="@name"/>
          </xsl:when>
          <xsl:otherwise><emphasis>implicit</emphasis></xsl:otherwise>
        </xsl:choose>
      </td>
      <td>
        <xsl:value-of select="@paramType"/>
      </td>
      <td>
        <xsl:choose>
          <xsl:when test="@description">
          <xsl:value-of select="@description"/>
          </xsl:when>
          <xsl:otherwise><emphasis>none</emphasis></xsl:otherwise>
        </xsl:choose>
      </td>
      <td>
        <xsl:value-of select="@required"/>
      </td>
      <td>
        <xsl:choose>
        <xsl:when test="starts-with(@typeId,'...')">
          <link>
            <xsl:attribute name="linkend">
                <xsl:value-of select="@typeId"/>
            </xsl:attribute>
            <xsl:value-of select="@type"/>
          </link>
        </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="@type"/>
          </xsl:otherwise>
        </xsl:choose>
      </td>
      <td>
        <xsl:choose>
          <xsl:when test="@allowableValues">
            <xsl:value-of select="@allowableValues"/>
          </xsl:when>
          <xsl:otherwise><emphasis>-all-</emphasis></xsl:otherwise>
        </xsl:choose>
      </td>
      <td>
        <xsl:choose>
          <xsl:when test="@defaultValue">
            <xsl:value-of select="@defaultValue"/>
          </xsl:when>
          <xsl:otherwise><emphasis>none</emphasis></xsl:otherwise>
        </xsl:choose>
      </td>
    </tr>
  </xsl:template>

  <xsl:template match="error">
    <tr>
      <td>
        <xsl:value-of select="@code"/>
      </td>
      <td>
        <xsl:value-of select="@reason"/>
      </td>
    </tr>
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

  <!-- emit media types produced -->
  <xsl:template name="print-created-media-types">
    <xsl:param name="produces"/>

    <xsl:if test="$produces">
      <simpara>
        Produces:
        <xsl:for-each select="$produces/type">
          <xsl:value-of select="."/>
          <xsl:if test="not(position() = last())">
            <xsl:text>, </xsl:text>
          </xsl:if>
        </xsl:for-each>
      </simpara>
    </xsl:if>

  </xsl:template>

  <xsl:template match="data">
    <xsl:element name="section">
      <xsl:attribute name="xml:id">
          <xsl:value-of select="@nameId"/>
      </xsl:attribute>
      <title>Data-Class: <xsl:value-of select="@name"/></title>
      <xsl:if test="@abstract">
        <subtitle><xsl:value-of select="@abstract"/></subtitle>
      </xsl:if>
      <xsl:if test="@description">
        <simpara>
          Description: <xsl:value-of select="@description"/>
        </simpara>
      </xsl:if>

          <table>
            <title>Properties</title>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Description</th>
            </tr>
            <xsl:apply-templates/>
          </table>
    </xsl:element>
  </xsl:template>

  <xsl:template match="property">
    <tr>
      <td>
        <xsl:value-of select="@name"/>
      </td>
      <td>
        <xsl:value-of select="@type"/>
      </td>
      <td>
        <xsl:value-of select="@description"/>
      </td>
    </tr>
  </xsl:template>

</xsl:stylesheet>
