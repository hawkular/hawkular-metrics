/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.dropwizard;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.common.base.Preconditions;

/**
 * Provides some basic functions for DropWizard metric names. In order to avoid naming collisions, each metric name
 * is prefixed with the hostname, which defaults to the hostname of the local host. All metric names should be created
 * using {@link #createMetricName(MetaData)} in order to ensure a consistent format.
 *
 * @author jsanda
 */
public class MetricNameService {

    public static final String separator = "|";

    private final String adminTenant;
    private final String hostname;

    public MetricNameService() {
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get hostname", e);
        }

        this.adminTenant = "admin";
    }

    public MetricNameService(String adminTenant) {
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get hostname", e);
        }

        this.adminTenant = adminTenant;
    }

    public MetricNameService(String hostname, String adminTenant) {
        this.hostname = hostname;
        this.adminTenant = adminTenant;
    }

    public String getHostName() {
        return hostname;
    }

    public String getTenantId() {
        return adminTenant;
    }

    /**
     * The format for a fully qualified metric name, which must be unique, is:
     * <br/><br/>
     * hostname|namespace|name
     */
    public String createMetricName(MetaData metaData) {
        Preconditions.checkArgument(!metaData.getName().contains(separator),
                metaData.getName() + " is an invalid metric name. Metric names cannot include " + separator);
        return hostname + separator +  metaData.getNamespace() + separator + metaData.getName();
    }

}
