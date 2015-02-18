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
package org.hawkular.metrics.clients.ptrans;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates supported services.
 *
 * @author Thomas Segismont
 */
public enum Service {
    /** **/
    UDP("udp"),
    /** **/
    TCP("tcp"),
    /** **/
    GANGLIA("ganglia"),
    /** **/
    STATSD("statsd"),
    /** **/
    COLLECTD("collectd");

    private final String externalForm;

    Service(String externalForm) {
        this.externalForm = externalForm;
    }

    /**
     * @return string representation of this service
     */
    public String getExternalForm() {
        return externalForm;
    }

    private static final Map<String, Service> SERVICES_BY_ID = new HashMap<>();

    static {
        for (Service service : Service.values()) {
            SERVICES_BY_ID.put(service.externalForm, service);
        }
    }

    /**
     * @param externalForm service string representation
     *
     * @return the {@link org.hawkular.metrics.clients.ptrans.Service} which externalForm is {@code externalForm}, null
     * otherwise
     */
    public static Service findByExternalForm(String externalForm) {
        return SERVICES_BY_ID.get(externalForm);
    }
}
