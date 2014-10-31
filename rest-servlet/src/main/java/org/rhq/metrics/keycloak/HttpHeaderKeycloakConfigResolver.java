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

import org.keycloak.adapters.HttpFacade;

/**
 * Resolves the realm for the request based on a HTTP Header.
 * <p>
 * Each incoming request should have an HTTP header named <code>X-RHQ-Realm</code> to inform the backend about which
 * tenant the request is intended for. Based on this header, the appropriate Keycloak configuration file is loaded.
 *
 * @author Juraci Paixão Kröhling <juraci at kroehling.de>
 */
public class HttpHeaderKeycloakConfigResolver extends BaseKeycloakConfigResolver {
  @Override
  public String getTenantId(HttpFacade.Request request) {
    return request.getHeader("X-RHQ-Realm");
  }
}
