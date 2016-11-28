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
package org.hawkular.metrics.api.jaxrs.util;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.servlet.ServletContext;

import com.google.common.collect.ImmutableList;

@ApplicationScoped
public class ManifestInformation {
    private static final List<String> VERSION_ATTRIBUTES = ImmutableList.of("Implementation-Version",
            "Built-From-Git-SHA1");

    @Inject
    private ServletContext servletContext;

    private Map<String, String> attributes = new HashMap<>();

    @PostConstruct
    public void init() {
        try (InputStream inputStream = servletContext.getResourceAsStream("/META-INF/MANIFEST.MF")) {
            Manifest manifest = new Manifest(inputStream);
            Attributes attr = manifest.getMainAttributes();
            for (String attribute : VERSION_ATTRIBUTES) {
                attributes.put(attribute, attr.getValue(attribute));
            }
        } catch (Exception e) {
            for (String attribute : VERSION_ATTRIBUTES) {
                if (attributes.get(attribute) == null) {
                    attributes.put(attribute, "Unknown");
                }
            }
        }
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }


}
