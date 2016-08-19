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
package org.hawkular.metrics.component.publish;

import java.util.List;

import org.hawkular.bus.common.AbstractMessage;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A bus message that will indicate which metrics will be published/unpublished to the bus.
 *
 * @author Lucas Ponce
 */
public class PublishCommandMessage extends AbstractMessage {

    public static final String PUBLISH_COMMAND = "publish";
    public static final String UNPUBLISH_COMMAND = "unpublish";

    @JsonInclude
    String command;

    @JsonInclude
    String tenantId;

    @JsonInclude
    List<MetricKey> ids;

    protected PublishCommandMessage(){
    }

    public PublishCommandMessage(String command, String tenantId, List<MetricKey> ids) {
        this.command = command;
        this.tenantId = tenantId;
        this.ids = ids;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public List<MetricKey> getIds() {
        return ids;
    }

    public void setIds(List<MetricKey> ids) {
        this.ids = ids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PublishCommandMessage that = (PublishCommandMessage) o;

        if (command != null ? !command.equals(that.command) : that.command != null) return false;
        if (tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null) return false;
        return ids != null ? ids.equals(that.ids) : that.ids == null;

    }

    @Override
    public int hashCode() {
        int result = command != null ? command.hashCode() : 0;
        result = 31 * result + (tenantId != null ? tenantId.hashCode() : 0);
        result = 31 * result + (ids != null ? ids.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PublishCommandMessage{" +
                "command='" + command + '\'' +
                ", tenantId='" + tenantId + '\'' +
                ", ids=" + ids +
                '}';
    }

    public static class MetricKey {
        @JsonInclude
        private String type;
        @JsonInclude
        private String id;

        public MetricKey() {
        }

        public MetricKey(String type, String id) {
            this.type = type;
            this.id = id;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MetricKey metricKey = (MetricKey) o;

            if (type != null ? !type.equals(metricKey.type) : metricKey.type != null) return false;
            return id != null ? id.equals(metricKey.id) : metricKey.id == null;

        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (id != null ? id.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "MetricKey{" +
                    "type='" + type + '\'' +
                    ", id='" + id + '\'' +
                    '}';
        }
    }
}
