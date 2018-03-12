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
package org.hawkular.metrics.core.service.transformers;

import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.joda.time.Duration;

import com.datastax.driver.core.Row;

import rx.Observable;
import rx.Observable.Transformer;

/**
 * Transforms {@link Row}s from data table to a {@link Metric}. Requires the following order on select:
 * {@code tenant_id, type, metric_id}.
 *
 * @author Thomas Segismont
 */
public class MetricIdentifierFromFullDataRowTransformer implements Transformer<Row, MetricId<?>> {
    private final int defaultDataRetention;

    public MetricIdentifierFromFullDataRowTransformer(int defaultTTL) {
        this.defaultDataRetention = (int) Duration.standardSeconds(defaultTTL).getStandardDays();
    }

    @Override
    public Observable<MetricId<?>> call(Observable<Row> rows) {
        return rows.map(row -> new MetricId<>(row.getString(0), MetricType.fromCode(row.getByte(1)),
                row.getString(2)));
    }
}
