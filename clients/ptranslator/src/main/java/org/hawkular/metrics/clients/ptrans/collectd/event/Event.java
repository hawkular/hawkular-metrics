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
package org.hawkular.metrics.clients.ptrans.collectd.event;

import static org.hawkular.metrics.clients.ptrans.util.Arguments.checkArgument;

/**
 * Base class for events decoded by {@link CollectdEventsDecoder}.
 *
 * @author Thomas Segismont
 * @see ValueListEvent
 */
public abstract class Event {
    private final String host;
    private final TimeSpan timestamp;
    private final String pluginName;
    private final String pluginInstance;
    private final String typeName;
    private final String typeInstance;

    protected Event(
            String host, TimeSpan timestamp, String pluginName, String pluginInstance, String typeName,
            String typeInstance
    ) {
        checkArgument(host != null, "host is null");
        checkArgument(timestamp != null, "timestamp is null");
        checkArgument(pluginName != null, "pluginName is null");
        checkArgument(pluginInstance != null, "pluginInstance is null");
        checkArgument(typeName != null, "typeName is null");
        checkArgument(typeInstance != null, "typeInstance is null");
        this.host = host;
        this.timestamp = timestamp;
        this.pluginName = pluginName;
        this.pluginInstance = pluginInstance;
        this.typeName = typeName;
        this.typeInstance = typeInstance;
    }

    /**
     * @return hostname of the machine where the values were collected, not null
     */
    public final String getHost() {
        return host;
    }

    /**
     * @return when they were collected, not null
     */
    public final TimeSpan getTimestamp() {
        return timestamp;
    }

    /**
     * @return the corresponding collectd plugin, e.g. "cpu", not null
     */
    public final String getPluginName() {
        return pluginName;
    }

    /**
     * @return the corresponding collectd plugin instance, e.g. "1", not null
     */
    public final String getPluginInstance() {
        return pluginInstance;
    }

    /**
     * @return the corresponding collectd type, e.g. "cpu", not null
     */
    public final String getTypeName() {
        return typeName;
    }

    /**
     * @return the corresponding collectd type instance, e.g. "idle", not null
     */
    public final String getTypeInstance() {
        return typeInstance;
    }

    @Override
    public String toString() {
        return "Event[" + "host='" + host + '\'' + ", timestamp=" + TimeResolution.toDate(timestamp) + ", pluginName='"
               + pluginName + '\'' + ", pluginInstance='" + pluginInstance + '\'' + ", typeName='" + typeName + '\''
               + ", typeInstance='" + typeInstance + '\'' + ']';
    }
}
