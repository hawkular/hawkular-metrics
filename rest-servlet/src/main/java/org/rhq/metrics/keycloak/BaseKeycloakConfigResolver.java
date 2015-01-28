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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.keycloak.adapters.HttpFacade;
import org.keycloak.adapters.KeycloakConfigResolver;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;

/**
 * This abstract class serves as the base point for implementing a RHQ-specific Keycloak Config Resolver. It
 * delegates the resolution of the realm name (tenant ID) to the subclass.
 *
 * If the system property <code>rhq.keycloak.config.location</code> is specified, the Keycloak configuration files are
 * read from this location. Otherwise, it tries to load the Keycloak configuration files from the root of the classpath.
 * <p>
 * The configuration files are cached locally, so, changes to the local configuration files are not detected after the
 * first read.
 * <p>
 * The files should be named following the pattern <code>keycloak-${REALM}.json</code>. For example, for realm
 * <code>acme</code>, the file <code>keycloak-acme.json</code> should be available.
 *
 * @author Juraci Paixão Kröhling <juraci at kroehling.de>
 */
public abstract class BaseKeycloakConfigResolver implements KeycloakConfigResolver {

  // TODO: this would cause a problem in a setup with a large number of tenants.
  // So, it would be better to either:
  // 1) use WildFly's cache provider (preferred)
  // 2) implement some sort of eviction, based on the least used entries
  // Besides that, we might want to consider re-loading a configuration file from the file system if we detect some
  // change. Perhaps adding the mtime to the cache entry? Perhaps doing a forced eviction after N minutes?
  private final Map<String, KeycloakDeployment> cache = new ConcurrentHashMap<>();

  @Override
  public KeycloakDeployment resolve(HttpFacade.Request facade) {
    String realm = getTenantId(facade);

    if (realm == null) {
      realm = System.getProperty("rhq.keycloak.config.default-realm", "rhq-master");
    }

    KeycloakDeployment deployment = cache.get(realm);
    if (null == deployment) {
      String keycloakConfigLocation = System.getProperty("rhq.keycloak.config.location");
      InputStream configurationStream = null;

      if (null == keycloakConfigLocation || keycloakConfigLocation.isEmpty()) {
        // try to load the configuration files from the classpath
        configurationStream = getClass().getResourceAsStream("/keycloak-" + realm + ".json");
      } else {
        // the config location was specified, try to load it from this location
        try {
          String name = keycloakConfigLocation + File.separator + "keycloak-" + realm + ".json";
          configurationStream = new FileInputStream(name);
        } catch (FileNotFoundException ex) {
          String message = "Couldn't load configuration for realm " + realm + ". Reason: " + ex.getMessage();
          throw new IllegalStateException(message);
        }
      }

      if (configurationStream == null) {
        throw new IllegalStateException("Not able to find the file /keycloak-" + realm + ".json");
      }

      deployment = KeycloakDeploymentBuilder.build(configurationStream);
      cache.put(realm, deployment);
    }

    return deployment;

  }

  public abstract String getTenantId(HttpFacade.Request request);

}
