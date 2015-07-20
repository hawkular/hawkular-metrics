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

import com.datastax.driver.core.ResultSet;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.impl.tags.MetricIndex;
import rx.Observable;

/**
 * Transforms ResultSets from metrics_tags_idx to a MetricIndex. Requires the following order on select:
 * type, metric, interval
 *
 * HWKMETRICS-114 might require changes to this
 *
 * @author Michael Burman
 */
public class TagsIndexResultSetTransformer implements Observable.Transformer<ResultSet, MetricIndex> {

    private MetricType type;

    public TagsIndexResultSetTransformer(MetricType type) {
        this.type = type;
    }

    @Override
    public Observable<MetricIndex> call(Observable<ResultSet> resultSetObservable) {
        return resultSetObservable
                .flatMap(Observable::from)
                .filter(r -> (type == null
                        && MetricType.userTypes().contains(MetricType.fromCode(r.getInt(0))))
                        || MetricType.fromCode(r.getInt(0)) == type)
                .map(r -> new MetricIndex(MetricType.fromCode(r.getInt(0)), new MetricId(r.getString
                        (1), Interval.parse(r.getString(2)))));
    }
}
