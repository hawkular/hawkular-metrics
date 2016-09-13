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
package org.hawkular.metrics.core.service;

import java.io.Serializable;

/**
 * We use the org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile class for computing percentiles. The
 * order in which values are added to PSquarePercentile can effect the results. This can make automated testing
 * difficult in scenarios in which values are added from different threads concurrently. For those scenarios
 * org.apache.commons.math3.stat.descriptive.rank.Percentile works better as it stores all values in memory and the
 * order in which it consumes values does not effect the result. Unfortunately, these two classes do not share a common
 * interface that we can use, so interface is used to facilitate testing.
 *
 * @author jsanda
 */
public interface PercentileWrapper extends Serializable {

    void addValue(double value);

    double getResult();

    void clear();

}
