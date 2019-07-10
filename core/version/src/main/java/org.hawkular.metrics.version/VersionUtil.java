/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.schema;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Manifest;

import com.google.common.base.Strings;

/**
 * @author jsanda
 */
public class VersionUtil {

    public static String getVersion() {
        try {
            Enumeration<URL> resources = VersionUtil.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                try (InputStream stream = resource.openStream()) {
                    Manifest manifest = new Manifest(stream);
                    String vendorId = manifest.getMainAttributes().getValue("Implementation-Vendor-Id");
                    String implVersion = manifest.getMainAttributes().getValue("Implementation-Version");
                    String gitSHA = manifest.getMainAttributes().getValue("Built-From-Git-SHA1");

                    if (isValidManifest(vendorId, implVersion, gitSHA)) {
                        return implVersion + "+" + gitSHA.substring(0, 10);
                    }
                }
            }
            throw new RuntimeException("Unable to determine implementation version for Hawkular Metrics");
        } catch (IOException e) {
            throw new RuntimeException("There was an I/O error reading META-INF/MANIFEST.MF", e);
        }
    }

    private static boolean isValidManifest(String vendorId, String implVersion, String gitSHA) {
        return vendorId != null && vendorId.equals("org.hawkular.metrics") &&
                !(Strings.isNullOrEmpty(implVersion) || Strings.isNullOrEmpty(gitSHA));
    }

}
