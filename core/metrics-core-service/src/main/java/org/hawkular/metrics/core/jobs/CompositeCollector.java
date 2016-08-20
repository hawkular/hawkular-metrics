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
package org.hawkular.metrics.core.jobs;

import java.util.Map;

import org.hawkular.metrics.core.service.cache.RollupKey;
import org.hawkular.metrics.core.service.transformers.NumericDataPointCollector;
import org.hawkular.metrics.model.DataPoint;
import org.jboss.logging.Logger;

/**
 * @author jsanda
 */
public class CompositeCollector {

    private static Logger logger = Logger.getLogger(CompositeCollector.class);

    private Map<RollupKey, NumericDataPointCollector> collectors;

    public CompositeCollector(Map<RollupKey, NumericDataPointCollector> collectors) {
        this.collectors = collectors;
    }

    public void increment(DataPoint<? extends Number> dataPoint) {
        logger.debug("DATA POINT = " + dataPoint);
        collectors.values().forEach(collector -> collector.increment(dataPoint));
    }

    public Map<RollupKey, NumericDataPointCollector> getCollectors() {
        return collectors;
    }

}
