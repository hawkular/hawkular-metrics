/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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


package org.hawkular.metrics

import static org.junit.Assert.assertEquals

import org.junit.BeforeClass
import org.junit.Test

import groovy.json.internal.Charsets
import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient

/**
 * @author jsanda
 */
class AuthenticationITest {

  static String testUser = 'jdoe'
  static String testPasword = 'password'
  static String baseURI
  static Closure defaultFailureHandler
  static String protectedEndpoint

  @BeforeClass
  static void initClient() {
    defaultFailureHandler = { resp ->
      def msg = "Got error response: ${resp.statusLine}"
      if (resp.entity != null && resp.entity.contentLength != 0) {
        def baos = new ByteArrayOutputStream()
        resp.entity.writeTo(baos)
        def entity = new String(baos.toByteArray(), Charsets.UTF_8)
        msg = """${msg}
=== Response body
${entity}
===
"""
      }
      System.err.println(msg)
      return resp
    }

    String host = System.getProperty('hawkular.bind.address') ?: 'localhost'
    if ("0.0.0.0".equals(host)) {
      host = "localhost"
    }
    int portOffset = Integer.parseInt(System.getProperty('hawkular.port.offset') ?: '0')
    int httpPort = portOffset + 8080
    baseURI = "http://${host}:${httpPort}/hawkular/metrics/"

    protectedEndpoint = 'gauges'
  }

  @Test
  void noCredentialsShouldFail() {
    def client = createClientWithoutCredentials()

    def response = client.get(path: protectedEndpoint)

    assertEquals(500, response.status)
  }

  @Test
  void invalidCredentialsShouldFail() {
    def client = createClient(testUser, 'badPassword')

    def response = client.get(path: protectedEndpoint)

    assertEquals(401, response.status)
  }

  @Test
  void validCredentialsShouldSucceed() {
    def client = createClient(testUser, testPasword)

    def response = client.get(path: protectedEndpoint)

    assertEquals(204, response.status)
  }

  @Test
  void requestWithCredentialsAndTenantHeaderIsInvalid() {
    def client = createClient(testUser, testPasword)

    def response = client.get(path: protectedEndpoint, headers: ['Hawkular-Tenant': 'test'])

    assertEquals(400, response.status)
  }

  @Test
  void statusEndpointShouldNotRequireAuthentication() {
    def client = createClientWithoutCredentials()

    def response = client.get(path: 'status')

    assertEquals(200, response.status)
  }

  @Test
  void rootNotRequireAuthentication() {
    def client = createClientWithoutCredentials()

    def response = client.get(path: '')

    assertEquals(200, response.status)
  }

  @Test
  void influxShouldRejectInvalidDatabaseIdentifier() {
    def client = createClientWithoutCredentials()

    def response = client.get(path: 'db/invalid_tenant/series', query: [q: 'list series'])

    assertEquals(400, response.status)
  }

  @Test
  void influxShouldRejectInvalidDatabaseFormat() {
    def client = createClientWithoutCredentials()
    def response = client.get(path: 'db/invalid_tenant/series', query: [q: 'list series'])
    assertEquals(400, response.status)
  }

  @Test
  void influxShouldAuthenticateUserWithRequestParams() {
    def client = createClient('jdoe', 'password')
    def response = client.get(path: 'http://localhost:8080/hawkular/accounts/personas/current')
    assertEquals(200, response.status)

    def unauthenticatedClient = createClientWithoutCredentials()
    def tenantId = response.data.id
    response = unauthenticatedClient.get(path: "db/${tenantId}/series", query: [q: 'list series', u: 'jdoe', p: 'password'])
    assertEquals(200, response.status)
  }

  static RESTClient createClient(String username, String password) {
    /* http://en.wikipedia.org/wiki/Basic_access_authentication#Client_side :
     * The Authorization header is constructed as follows:
     *  * Username and password are combined into a string "username:password"
     *  * The resulting string is then encoded using the RFC2045-MIME variant of Base64,
     *    except not limited to 76 char/line[9]
     *  * The authorization method and a space i.e. "Basic " is then put before the encoded string.
     */
    String encodedCredentials = Base64.getMimeEncoder().encodeToString("$username:$password".getBytes("utf-8"))
    RESTClient client = new RESTClient(baseURI, ContentType.JSON)
    client.defaultRequestHeaders.Authorization = "Basic " + encodedCredentials
    client.defaultRequestHeaders.Accept = ContentType.JSON
    client.handler.failure = defaultFailureHandler

    return client
  }

  static RESTClient createClientWithoutCredentials() {
    RESTClient client = new RESTClient(baseURI, ContentType.JSON)
    client.defaultRequestHeaders.Accept = ContentType.JSON
    client.handler.failure = defaultFailureHandler

    return client
  }

}
