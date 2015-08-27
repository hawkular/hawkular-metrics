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
package org.hawkular.metrics.api.jaxrs.util;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import javax.servlet.ServletContext;

public class ManifestInformation {

    public static final List<String> VERSION_ATTRIBUTES = Collections
            .unmodifiableList(Arrays.asList("Implementation-Version", "Built-From-Git-SHA1"));

    public static Map<String, String> getManifestInformation(ServletContext servletContext, String... attributes) {
        return getManifestInformation(servletContext, Arrays.asList(attributes));
    }

    public static Map<String, String> getManifestInformation(ServletContext servletContext, List<String> attributes) {
        Map<String, String> manifestAttributes = new HashMap<>();
        try (InputStream inputStream = servletContext.getResourceAsStream("/META-INF/MANIFEST.MF")) {
            Manifest manifest = new Manifest(inputStream);
            Attributes attr = manifest.getMainAttributes();
            for (String attribute : attributes) {
                manifestAttributes.put(attribute, attr.getValue(attribute));
            }
        } catch (Exception e) {
            for (String attribute : attributes) {
                if (manifestAttributes.get(attribute) == null) {
                    manifestAttributes.put(attribute, "Unknown");
                }
            }
        }

        return manifestAttributes;
    }
}
