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
package org.hawkular.metrics.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

/**
 * @author jsanda
 */
public class Status {

    private String metricsServiceStatus;

    private String gitSHA;

    private String implementationVersion;

    private List<CassandraStatus> cassandraStatusList;

    @JsonProperty("MetricsService")
    public String getMetricsServiceStatus() {
        return metricsServiceStatus;
    }

    public void setMetricsServiceStatus(String metricsServiceStatus) {
        this.metricsServiceStatus = metricsServiceStatus;
    }

    @JsonProperty("Built-From-Git-SHA1")
    public String getGitSHA() {
        return gitSHA;
    }

    public void setGitSHA(String gitSHA) {
        this.gitSHA = gitSHA;
    }

    @JsonProperty("Implementation-Version")
    public String getImplementationVersion() {
        return implementationVersion;
    }

    public void setImplementationVersion(String implementationVersion) {
        this.implementationVersion = implementationVersion;
    }

    @JsonProperty("Cassandra")
    public List<CassandraStatus> getCassandraStatus() {
        return cassandraStatusList;
    }

    public void setCassandraStatus(List<CassandraStatus> cassandraStatusList) {
        this.cassandraStatusList = cassandraStatusList;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("metricsServiceStatus", metricsServiceStatus)
                .add("gitSHA", gitSHA)
                .add("implementationVersion", implementationVersion)
                .add("cassandraStatusList", cassandraStatusList)
                .toString();
    }
}
