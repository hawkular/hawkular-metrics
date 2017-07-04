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
package org.hawkular.metrics.core.service.tags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.PatternUtil;
import org.hawkular.metrics.core.service.transformers.ItemsToSetTransformer;
import org.hawkular.metrics.core.service.transformers.TagsIndexRowTransformerFilter;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;

import com.datastax.driver.core.Row;

import rx.Observable;
import rx.functions.Func1;

/**
 * Very simple query optimizer and parser for the tags query language
 *
 * @author Michael Burman
 */
public class SimpleTagQueryParser {

    private DataAccess dataAccess;
    private MetricsService metricsService;

    public SimpleTagQueryParser(DataAccess dataAccess, MetricsService metricsService) {
        this.dataAccess = dataAccess;
        this.metricsService = metricsService;
    }

    // Arrange the queries:
    // a) Group which can be executed on Cassandra
    // b) Group which requires Cassandra and in-memory
    // c) Group which is executed in memory by fetching metric definitions from Cassandra
    // Arrange each group to a sequence of most powerful query (such as exact query on Cassandra)

    // Between group b & c, fetch the metric definitions (of the current list)

    static class QueryOptimizer {

        public static final long GROUP_A_COST = 10;
        public static final long GROUP_B_COST = 50;
        public static final long GROUP_C_COST = 99;

        /**
         * Sorts the query language parameters based on their query cost.
         *
         * @param tagsQuery User's TagQL
         * @return TreeMap with Long key indicating query cost (lower is better)
         */
        public static Map<Long, List<Map.Entry<String, String>>> reOrderTagsQuery(Map<String, String> tagsQuery) {
            Map<Long, List<Map.Entry<String, String>>> costSortedMap = new TreeMap<>();
            costSortedMap.put(GROUP_B_COST, new ArrayList<>());
            costSortedMap.put(GROUP_C_COST, new ArrayList<>());

            for (Map.Entry<String, String> tagQuery : tagsQuery.entrySet()) {
                if(tagQuery.getKey().startsWith("!")) {
                    // In-memory sorted query, requires fetching all the definitions
                    List<Map.Entry<String, String>> entries = costSortedMap.get(GROUP_C_COST);
                    entries.add(tagQuery);
                } else {
                    // TODO How to filter exact matching from regexp matching?
                    List<Map.Entry<String, String>> entries = costSortedMap.get(GROUP_B_COST);
                    entries.add(tagQuery);
                }
            }

            return costSortedMap;
        }
    }

    public Observable<MetricId<?>> findMetricIdentifiersWithFilters(String tenantId, MetricType<?> metricType,
                                                                    Map<String, String> tagsQueries) {

        Map<Long, List<Map.Entry<String, String>>> costSortedMap = QueryOptimizer.reOrderTagsQuery(tagsQueries);

        List<Map.Entry<String, String>> groupBEntries = costSortedMap.get(QueryOptimizer.GROUP_B_COST);
        List<Map.Entry<String, String>> groupCEntries = costSortedMap.get(QueryOptimizer.GROUP_C_COST);

        Observable<MetricId<?>> groupMetrics;

        // Fetch everything from the tagsQueries
        groupMetrics = Observable.from(groupBEntries)
                        .flatMap(e -> dataAccess.findMetricsByTagName(tenantId, e.getKey())
                                .filter(tagValueFilter(e.getValue(), 3))
                                .compose(new TagsIndexRowTransformerFilter<>(metricType))
                                .compose(new ItemsToSetTransformer<>())
                                .reduce((s1, s2) -> {
                                    s1.addAll(s2);
                                    return s1;
                                }))
                        .reduce((s1, s2) -> {
                            s1.retainAll(s2);
                            return s1;
                        })
                        .flatMap(Observable::from);
//                        .flatMap(metricsService::findMetric);

        // There might not be any metrics fetched yet.. if this is the only query
        if(groupBEntries.isEmpty() && !groupCEntries.isEmpty()) {
            // Fetch all the available metrics for this tenant
            Observable<? extends MetricId<?>> tagsMetrics = dataAccess.findAllMetricsFromTagsIndex()
                    .compose(new TagsIndexRowTransformerFilter<>(metricType))
                    .filter(mId -> mId.getTenantId().equals(tenantId));

            Observable<MetricId<?>> dataMetrics = metricsService.findAllMetricIdentifiers()
                    .filter(m -> m.getTenantId().equals(tenantId))
                    .filter(metricTypeFilter(metricType));

            groupMetrics = Observable.concat(tagsMetrics, dataMetrics).distinct();
        }

        // Group C processing, everything outside Cassandra
        if(groupCEntries.size() > 0) {
            // TODO zipWith or something here instead of this monstrosity
            Observable<Metric<?>> metrics = groupMetrics.flatMap(metricsService::findMetric);

            for (Map.Entry<String, String> groupCQuery : groupCEntries) {
                metrics = metrics
                        .filter(tagNotExistsFilter(groupCQuery.getKey().substring(1)));
            }
            groupMetrics = metrics.map(Metric::getMetricId);
        }

        return groupMetrics;
    }

    public Observable<Map<String, Set<String>>> getTagValues(String tenantId, MetricType<?> metricType,
                                                             Map<String, String> tagsQueries) {

        // Row: 0 = type, 1 = metricName, 2 = tagValue, e.getKey = tagName, e.getValue = regExp
        return Observable.from(tagsQueries.entrySet())
                .flatMap(e -> dataAccess.findMetricsByTagName(tenantId, e.getKey())
                        .filter(typeFilter(metricType, 1))
                        .filter(tagValueFilter(e.getValue(), 3))
                        .map(row -> {
                            Map<String, Map<String, String>> idMap = new HashMap<>();
                            Map<String, String> valueMap = new HashMap<>();
                            valueMap.put(e.getKey(), row.getString(3));

                            idMap.put(row.getString(2), valueMap);
                            return idMap;
                        })
                        .switchIfEmpty(Observable.just(new HashMap<>()))
                        .reduce((map1, map2) -> {
                            map1.putAll(map2);
                            return map1;
                        }))
                .reduce((m1, m2) -> {
                    // Now try to emulate set operation of cut
                    Iterator<Map.Entry<String, Map<String, String>>> iterator = m1.entrySet().iterator();

                    while (iterator.hasNext()) {
                        Map.Entry<String, Map<String, String>> next = iterator.next();
                        if (!m2.containsKey(next.getKey())) {
                            iterator.remove();
                        } else {
                            // Combine the entries
                            Map<String, String> map2 = m2.get(next.getKey());
                            map2.forEach((k, v) -> next.getValue().put(k, v));
                        }
                    }

                    return m1;
                })
                .map(m -> {
                    Map<String, Set<String>> tagValueMap = new HashMap<>();

                    m.forEach((k, v) ->
                            v.forEach((subKey, subValue) -> {
                                if (tagValueMap.containsKey(subKey)) {
                                    tagValueMap.get(subKey).add(subValue);
                                } else {
                                    Set<String> values = new HashSet<>();
                                    values.add(subValue);
                                    tagValueMap.put(subKey, values);
                                }
                            }));

                    return tagValueMap;
                });
    }

    public Observable<String> getTagNames(String tenantId, MetricType<?> metricType, String filter) {
        Observable<String> tagNames;
        if(metricType == null) {
            tagNames = dataAccess.getTagNames()
                    .filter(r -> tenantId.equals(r.getString(0)))
                    .map(r -> r.getString(1))
                    .distinct();
        } else {
            // This query is slower than without type - we have to request all the rows, not just partition keys
            tagNames = dataAccess.getTagNamesWithType()
                    .filter(typeFilter(metricType, 2))
                    .filter(r -> tenantId.equals(r.getString(0)))
                    .map(r -> r.getString(1))
                    .distinct();
        }
        return tagNames.filter(tagNameFilter(filter));
    }

    private Func1<Metric<?>, Boolean> tagNotExistsFilter(String unwantedTagName) {
        return tMetric -> !tMetric.getTags().keySet().contains(unwantedTagName);
    }

    private Func1<String, Boolean> tagNameFilter(String regexp) {
        if(regexp != null) {
            boolean positive = (!regexp.startsWith("!"));
            Pattern p = PatternUtil.filterPattern(regexp);
            return s -> positive == p.matcher(s).matches(); // XNOR
        }
        return s -> true;
    }

    private Func1<Row, Boolean> tagValueFilter(String regexp, int index) {
        boolean positive = (!regexp.startsWith("!"));
        Pattern p = PatternUtil.filterPattern(regexp);
        return r -> positive == p.matcher(r.getString(index)).matches(); // XNOR
    }

    public Func1<Row, Boolean> typeFilter(MetricType<?> type, int index) {
        return row -> {
            MetricType<?> metricType = MetricType.fromCode(row.getByte(index));
            return (type == null && metricType.isUserType()) || metricType == type;
        };
    }

    public Func1<MetricId<?>, Boolean> metricTypeFilter(MetricType<?> type) {
        return tMetricId -> (type == null && tMetricId.getType().isUserType()) || tMetricId.getType() == type;
    }
}
