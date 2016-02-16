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
import static org.junit.Assert.assertTrue

import org.junit.Test

/**
 * @author jsanda
 */
class AuthenticationITest extends BaseITest {
  static String protectedEndpoint = 'gauges'

  @Test
  void noCredentialsShouldFail() {
    def client = createClientWithoutCredentials()

    def response = client.get(path: protectedEndpoint)

    assertEquals(401, response.status)
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

    // Status can be 200 if another test ran before and inserted some gauge data points
    assertTrue([200, 204].contains(response.status))
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
    def unauthenticatedClient = createClientWithoutCredentials()
    def response = unauthenticatedClient.get(path: "db/${getTenantId()}/series", query: [q: 'list series', u: 'jdoe', p: 'password'])
    assertEquals(200, response.status)
  }

}
