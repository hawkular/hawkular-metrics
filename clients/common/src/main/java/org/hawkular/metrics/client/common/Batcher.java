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
package org.hawkular.metrics.client.common;

import java.util.Collection;
import java.util.Iterator;

/**
 * Just a helper
 * @author Heiko W. Rupp
 */
public class Batcher {

    private Batcher() {
        // Utility class
    }

    /**
     * Translate the passed collection of metrics into a JSON representation
     * @param metrics a Collection of metrics to translate
     * @return String as JSON representation of the Metrics.
     */
    public static String metricListToJson(final Collection<SingleMetric> metrics) {
            StringBuilder builder = new StringBuilder("[");
            Iterator<SingleMetric> iter = metrics.iterator();
            while (iter.hasNext()) {
                SingleMetric event = iter.next();
                builder.append(event.toJson());
                if (iter.hasNext()) {
                    builder.append(',');
                }
            }
            builder.append(']');

            return builder.toString();
        }

}
