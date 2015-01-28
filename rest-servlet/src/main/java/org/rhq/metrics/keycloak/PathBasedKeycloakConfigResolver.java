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
 *
 * @author Juraci Paixão Kröhling <juraci at kroehling.de>
 */
public class PathBasedKeycloakConfigResolver extends BaseKeycloakConfigResolver {

  /**
   * Returns the tenant ID for the given path. As convention, the path is something like this:
   * <code>/tenants/{tenant}/metrics/data</code>, so, this method just extracts the value for "tenant".
   *
   * If the tenant ID cannot be determined, returns null.
   *
   * @param request the request facade
   * @return the tenant ID
   */
  @Override
  public String getTenantId(HttpFacade.Request request) {
    String path = request.getURI();
    if (path.contains("/tenants/")) {
      path = path.substring(path.indexOf("/tenants/"));
      String[] parts = path.split("\\/");
      if (parts.length > 2) {
        return parts[2];
      }
    }
    return null;
  }

}
