/*
 * Copyright 2015 Red Hat, Inc.
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

package org.rhq.metrics.restServlet.config;

/**
 * Application configuration keys.
 *
 * @author Thomas Segismont
 * @see Configurable
 */
public enum ConfigurationKey {
    /**
     * Storage type. The value may be one of:
     * <ul>
     *     <li><em>mem</em> for memory</li>
     *     <li><em>cass</em> for Cassandra</li>
     * </ul>
     * Memory backend will be used whenever the value is not <em>cass</em>.
     */
    BACKEND("rhq-metrics.backend");

    private String externalForm;

    ConfigurationKey(String externalForm) {
        this.externalForm = externalForm;
    }

    public String getExternalForm() {
        return externalForm;
    }
}
