/*
 * Copyright 2014 Red Hat, Inc.
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

package org.rhq.metrics.restServlet;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;

import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;

/**
 * @author Thomas Segismont
 */
@MBean(category = "org.rhq.metrics", name = "BackendConfiguration", description = "RHQ Metrics Backend Configuration")
@ApplicationScoped
public class BackendConfigurationManagedBean {

    @JmxManaged(description = "Describes the metrics service backend type")
    private String backendType;

    @PostConstruct
    void init() {
        backendType = System.getProperty("rhq-metrics.backend");
        if (backendType == null || !backendType.equals("cass")) {
            backendType = "mem";
        }
    }

    public String getBackendType() {
        return backendType;
    }
}
