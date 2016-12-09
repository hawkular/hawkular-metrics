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

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hawkular.metrics.core.service.transformers.ItemsToSetTransformer;
import org.hawkular.metrics.core.service.transformers.TagsIndexRowTransformer;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricType;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import rx.Observable;

/**
 * JSON Path based tag query parser
 *
 * @author Stefan Negrea
 */
public class JsonTagQueryParser {

    public static final String JSON_PATH_PREFIX = "json:";
    private final int JSON_PATH_PREFIX_LENGTH = JSON_PATH_PREFIX.length();

    private DataAccess dataAccess;
    private MetricsService metricsService;

    public JsonTagQueryParser(DataAccess dataAccess, MetricsService metricsService) {
        this.dataAccess = dataAccess;
        this.metricsService = metricsService;

        Configuration.setDefaults(new Configuration.Defaults() {

            private final JsonProvider jsonProvider = new JacksonJsonProvider();
            private final MappingProvider mappingProvider = new JacksonMappingProvider();

            @Override
            public JsonProvider jsonProvider() {
                return jsonProvider;
            }

            @Override
            public MappingProvider mappingProvider() {
                return mappingProvider;
            }

            @Override
            public Set<Option> options() {
                return EnumSet.noneOf(Option.class);
            }
        });

    }

    public Observable<Metric<?>> findMetricsWithFilters(String tenantId, MetricType<?> metricType,
            Map<String, String> jsonTagQueries) {
        return Observable.from(jsonTagQueries.entrySet())
                .flatMap(e -> dataAccess.findMetricsByTagName(tenantId, e.getKey())
                        .filter(r -> jsonPathFilter(r.getString(3), e.getValue()))
                        .compose(new TagsIndexRowTransformer<>(metricType))
                        .compose(new ItemsToSetTransformer<>()))
                .flatMap(Observable::from)
                .flatMap(metricsService::findMetric);
    }

    @SuppressWarnings("rawtypes")
    private boolean jsonPathFilter(String text, String jsonPathFilter) {
        jsonPathFilter = jsonPathFilter.substring(JSON_PATH_PREFIX_LENGTH);

        try {
            Object reply = JsonPath.parse(text).read(jsonPathFilter);
            if (reply instanceof List) {
                return ((List) reply).size() > 0;
            } else {
                return reply != null;
            }

        } catch (Exception ex) {
            return false;
        }
    }
}
