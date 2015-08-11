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
package org.hawkular.metrics.core.impl.transformers;

import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;

import com.datastax.driver.core.Row;

import rx.Observable;

/**
 * Transforms ResultSets's Rows from metrics_tags_idx to a MetricId. Requires the following order on select:
 * type, metric, interval
 *
 * @author Michael Burman
 */
public class TagsIndexRowTransformer implements Observable.Transformer<Row, MetricId> {

    private MetricType type;
    private String tenantId;

    public TagsIndexRowTransformer(String tenantId, MetricType type) {
        this.type = type;
        this.tenantId = tenantId;
    }

    @Override
    public Observable<MetricId> call(Observable<Row> resultSetObservable) {
        return resultSetObservable
//                .flatMap(Observable::from)
                .filter(r -> (type == null
                        && MetricType.userTypes().contains(MetricType.fromCode(r.getInt(0))))
                        || MetricType.fromCode(r.getInt(0)) == type)
                .map(r -> new MetricId(tenantId, MetricType.fromCode(r.getInt(0)), r.getString(1)));
    }
}
