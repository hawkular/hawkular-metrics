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
package org.hawkular.metrics.core.service.tags;

import java.util.AbstractMap.SimpleImmutableEntry;

import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.MetricsService;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import rx.Observable;

/**
 * Base tag query parser
 *
 * @author Stefan Negrea
 */
public abstract class BaseTagQueryParser {
    protected final DataAccess dataAccess;
    protected final MetricsService metricsService;

    public BaseTagQueryParser(DataAccess dataAccess, MetricsService metricsService) {
        this.dataAccess = dataAccess;
        this.metricsService = metricsService;
    }

    protected Multimap<String, String> removePrefixes(Multimap<String, String> tagsQueries, String prefix) {
        return Observable.from(tagsQueries.entries())
                .map(e -> new SimpleImmutableEntry<String, String>(e.getKey(), removePrefix(e.getValue(), prefix)))
                .collect(ArrayListMultimap::<String, String> create, (m, e) -> m.put(e.getKey(), e.getValue()))
                .toBlocking().firstOrDefault(ArrayListMultimap.<String, String> create());
    }

    protected String removePrefix(String filter, String prefix) {
        if (filter.startsWith(prefix)) {
            return filter.substring(prefix.length());
        } else {
            return filter;
        }
    }
}
