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

import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;

import com.datastax.driver.core.Row;

import rx.Observable;
import rx.Observable.Transformer;

/**
 * Transforms {@link Row}s from metrics_tags_idx to a {@link MetricId}. Requires the following order on select:
 * {@code type, metric, interval}.
 *
 * @author Michael Burman
 */
public class TagsIndexRowTransformerFilter<T> implements Transformer<Row, MetricId<T>> {
    private final MetricType<T> type;

    public TagsIndexRowTransformerFilter(MetricType<T> type) {
        this.type = type;
    }

    @Override
    public Observable<MetricId<T>> call(Observable<Row> rows) {
        return rows.filter(row -> {
            MetricType<?> metricType = MetricType.fromCode(row.getByte(1));
            return (type == null && metricType.isUserType()) || metricType == type;
        }).map(row1 -> {
            @SuppressWarnings("unchecked")
            MetricType<T> metricType = (MetricType<T>) MetricType.fromCode(row1.getByte(1));
            return new MetricId<>(row1.getString(0), metricType, row1.getString(2));
        });
    }
}
