/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Map.Entry

import groovy.json.JsonSlurper

def baseFile = new File(baseFile)
if (!baseFile.canRead()) throw new RuntimeException("${baseFile.path} is not readable")

def swaggerFile = new File(swaggerFile)
if (!swaggerFile.canRead()) throw new RuntimeException("${swaggerFile.path} is not readable")

def jsonSlurper = new JsonSlurper()
def swagger = jsonSlurper.parse(swaggerFile)

def apidocFile = new File(outputFile)

baseFile.withInputStream { stream ->
  apidocFile.append(stream)
}

apidocFile.withWriterAppend('UTF-8') { writer ->

  writer.println """

== Base Path
`${swagger.basePath}`

== REST APIs

"""

  swagger.tags.sort { t1, t2 -> t1.name.compareTo(t2.name) }.each { tag ->
    writer.println "=== ${tag.name}"

    swagger.paths.sort { p1, p2 -> p1.key.compareTo(p2.key) }.each { Entry path ->
      path.value.each { Entry method ->
        if (method.value.tags.contains(tag.name)) {
          writeEndpointLink(writer, path, method)
        }
      }
    }

    swagger.paths.sort { p1, p2 -> p1.key.compareTo(p2.key) }.each { Entry path ->
      path.value.each { Entry method ->
        if (method.value.tags.contains(tag.name)) {

          writeEndpointHeader(writer, path, method)

          def params = (method.value.parameters ?: []).findAll { it.schema?.$ref != '#/definitions/AsyncResponse' }
          writeParams(writer, 'Path', params.findAll { it.in == 'path' })
          writeParams(writer, 'Query', params.findAll { it.in == 'query' })
          writeBodyParams(writer, params.findAll { it.in == 'body' })

          writeResponses(writer, method.value.responses ?: [:])

          writeEndpointFooter(writer)
        }
      }
    }
  }

  writer.println '''== Data Types

'''

  writeDataTypes(writer, swagger.definitions ?: [:])
}

private writeEndpointLink(Writer writer, Entry path, Entry method) {
  def summary = method.value.summary.trim()
  String endpoint = path.key
  def httpMethod = method.key.toUpperCase()
  Object anchor = getEndpointAnchor(httpMethod, endpoint)

  writer.println ". link:#++${anchor}++[${summary}]"
}

def String getEndpointAnchor(def httpMethod, String endpoint) {
  def anchor = httpMethod + '_' + endpoint.replace('/', '_').replace('{', '_').replace('}', '_')
  anchor
}

private writeEndpointHeader(Writer writer, Entry path, Entry method) {
  def summary = method.value.summary.trim()
  def description = method.value.description?.trim()
  String endpoint = path.key
  def httpMethod = method.key.toUpperCase()
  def anchor = getEndpointAnchor(httpMethod, endpoint)

  writer.println """

==============================================

[[${anchor}]]
*Endpoint ${httpMethod} `${endpoint}`*

NOTE: *${summary}* +
${description ? "_${description}_" : ''}

"""
}

def writeParams(Writer writer, String paramType, List params) {
  if (!params.empty) {
    writer.println """
*${paramType} parameters*

[cols="15,^10,35,^15,^10,^15", options="header"]
|=======================
|Parameter|Required|Description|Type|Format|Allowable Values
"""
    params.each { param ->
      def name = param.name
      def required = required(param.required)
      def description = param.description ?: '-'
      def type = param.type
      def format = param.format ?: '-'
      def allowableValues = allowableValues(param.enum)
      writer.println "|${name}|${required}|${description}|${type}|${format}|${allowableValues}"
    }
    writer.println '''
|=======================

'''
  }
}

def writeBodyParams(Writer writer, List params) {
  if (!params.empty) {
    writer.println """
*Body*

[cols="^20,55,^25", options="header"]
|=======================
|Required|Description|Data Type
"""
    params.each { param ->
      def schema = param.schema ?: [:]
      def required = required(param.required)
      def description = param.description ?: '-'
      writer.println "|${required}|${description}|${schemaToType(schema)}"
    }
    writer.println """
|=======================

"""
  }
}

def void writeResponses(Writer writer, Map responses) {
  if (!responses.empty) {
    writer.println """
*Response*

*Status codes*
[cols="^20,55,^25", options="header"]
|=======================
|Status Code|Reason|Response Model
"""
    responses.each { Entry response ->
      def code = response.key
      def description = response.value.description ?: '-'
      def schema = response.value.schema ?: [:]
      writer.println "|${code}|${description}|${schemaToType(schema)}"
    }
    writer.println """
|=======================

"""
  }
}

def writeEndpointFooter(Writer writer) {
  writer.println """
==============================================

"""
}

def writeDataTypes(Writer writer, Map definitions) {
  definitions.findAll { Entry definition -> definition.key != 'AsyncResponse' }.each { Entry definition ->
    def definitionName = definition.key
    writer.println """
[[${definitionName}]]
=== ${definitionName}
[cols="15,^10,35,^15,^10,^15", options="header"]
|=======================
|Name|Required|Description|Type|Format|Allowable Values
"""
    definition.value.properties.each { Entry property ->
      def propertyName = property.key
      def required = required((definition.value.required ?: []).contains(propertyName))
      def description = property.value.description ?: '-'
      def type
      switch (property.value.type) {
        case 'array':
          def items = property.value.items ?: [:]
          if (items['$ref']) {
            String itemsRef = items['$ref']
            type = "array of <<${itemsRef.substring("#/definitions/".length())}>>"
          } else {
            type = "array of ${items.type}"
          }
          break;
        default:
          type = property.value.type
      }
      def format = property.value.format ?: '-'
      def allowableValues = allowableValues(property.value.enum)
      writer.println "|${propertyName}|${required}|${description}|${type}|${format}|${allowableValues}"
    }
    writer.println '''
|=======================
'''
  }
}

def String required(boolean val) {
  val ? 'Yes' : 'No'
}

def String allowableValues(def allowed) {
  (allowed ?: []).join(', ') ?: '-'
}

def String schemaToType(Map schema) {
  def model
  if (schema['$ref']) {
    String ref = schema['$ref']
    model = "<<${ref.substring("#/definitions/".length())}>>"
  } else if (schema.type) {
    switch (schema.type) {
      case "array":
        def items = schema.items ?: [:]
        if (items['$ref']) {
          String itemsRef = items['$ref']
          model = "array of <<${itemsRef.substring("#/definitions/".length())}>>"
        } else {
          model = "array of ${items.type}"
        }
        break;
      default:
        model = schema.type
    }
  } else {
    model = '-'
  }
  return model
}
