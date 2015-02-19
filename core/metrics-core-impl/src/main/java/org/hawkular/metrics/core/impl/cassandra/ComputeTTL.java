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
package org.hawkular.metrics.core.impl.cassandra;

import java.util.List;

import com.google.common.base.Function;

import org.hawkular.metrics.core.api.MetricData;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * @author John Sanda
 */
public class ComputeTTL<T extends MetricData> implements Function<List<T>, List<T>> {

    private int originalTTL;

    public ComputeTTL(int originalTTL) {
        this.originalTTL = originalTTL;
    }

    @Override
    public List<T> apply(List<T> data) {
        for (T d : data) {
            Duration duration = new Duration(DateTime.now().minus(d.getWriteTime()).getMillis());
            d.setTTL(originalTTL - duration.toStandardSeconds().getSeconds());
        }
        return data;
    }

}
