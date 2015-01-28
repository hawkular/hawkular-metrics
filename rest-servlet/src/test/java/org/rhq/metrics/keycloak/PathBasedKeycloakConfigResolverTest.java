/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates
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
package org.rhq.metrics.keycloak;

import java.io.InputStream;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.keycloak.adapters.HttpFacade;

/**
 *
 * @author Juraci Paixão Kröhling <juraci at kroehling.de>
 */
public class PathBasedKeycloakConfigResolverTest {

  @Test
  public void validTenantFromPath() {
    PathBasedKeycloakConfigResolver resolver = new PathBasedKeycloakConfigResolver();
    assertEquals("acme", resolver.getTenantId(getRequestForPath("/tenants/acme/metrics/data")));
    assertEquals("acme", resolver.getTenantId(getRequestForPath("/tenants/acme")));
    assertEquals("acme", resolver.getTenantId(getRequestForPath("/tenants/acme/")));
    assertEquals("acme.withdot", resolver.getTenantId(getRequestForPath("/tenants/acme.withdot/metrics/data")));
    assertEquals("acme-withdash", resolver.getTenantId(getRequestForPath("/tenants/acme-withdash/metrics/data")));
    assertEquals("influxtest", resolver.getTenantId(getRequestForPath(
            "http://127.0.0.1:55977/rhq-metrics/tenants/influxtest/influx/series"
    )));
  }

  @Test
  public void tenantIsNullOnIncompletePath() {
    PathBasedKeycloakConfigResolver resolver = new PathBasedKeycloakConfigResolver();
    assertEquals(null, resolver.getTenantId(getRequestForPath("/tenants")));
    assertEquals(null, resolver.getTenantId(getRequestForPath("/tenants/")));
    assertEquals(null, resolver.getTenantId(getRequestForPath("/generic/")));
    assertEquals(null, resolver.getTenantId(getRequestForPath("/")));
  }

  public HttpFacade.Request getRequestForPath(String path) {
    return new HttpFacade.Request() {

      @Override
      public String getMethod() {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public String getURI() {
        return path;
      }

      @Override
      public boolean isSecure() {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public String getQueryParamValue(String param) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public HttpFacade.Cookie getCookie(String cookieName) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public String getHeader(String name) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public List<String> getHeaders(String name) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public InputStream getInputStream() {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public String getRemoteAddr() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }

}
