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
package org.hawkular.metrics.api.jaxrs.util;

import org.hawkular.metrics.model.MetricType;

/**
 * Returns the longer path format used by the REST-interface from the given MetricType
 *
 * @author Michael Burman
 */
public class MetricTypeTextConverter {

    private MetricTypeTextConverter() { }

    public static String getLongForm(MetricType<?> mt) {

        if(mt == MetricType.GAUGE) {
            return "gauges";
        } else if(mt == MetricType.COUNTER) {
            return "counters";
        } else if(mt == MetricType.AVAILABILITY) {
            return mt.getText();
        } else if (mt == MetricType.STRING) {
            return "strings";
        } else {
            throw new RuntimeException("Unsupported type " + mt.getText());
        }
    }
}
