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
 * Represents a time duration.
 *
 * @author Thomas Segismont
 */
public final class TimeSpan {
    private final long value;
    private final TimeResolution resolution;

    /**
     * @param value      time duration
     * @param resolution time resolution, cannot be null
     *
     * @see TimeResolution
     */
    public TimeSpan(long value, TimeResolution resolution) {
        checkArgument(resolution != null, "resolution is null");
        this.value = value;
        this.resolution = resolution;
    }

    public long getValue() {
        return value;
    }

    public TimeResolution getResolution() {
        return resolution;
    }

    @Override
    public String toString() {
        return "TimeSpan[" + "value=" + value + ", resolution=" + resolution + ']';
    }
}
