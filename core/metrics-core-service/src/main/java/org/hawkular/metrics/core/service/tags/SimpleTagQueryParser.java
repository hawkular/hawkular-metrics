/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
    private boolean enableACostQueries;

    public SimpleTagQueryParser(DataAccess dataAccess, MetricsService metricsService, boolean disableACostQueries) {
        this.dataAccess = dataAccess;
        this.metricsService = metricsService;
        this.enableACostQueries = !disableACostQueries;
    }

    static class Query {
        private String tagName;
        private String tagValueMatcher;
        private String[] tagValues;

        public Query(String tagName, String tagValueMatcher, String... tagValues) {
            this.tagName = tagName;
            if(tagValues.length > 0) {
                this.tagValues = tagValues;
            } else {
                this.tagValueMatcher = tagValueMatcher;
            }
        }

        public String getTagName() {
            return tagName;
        }

        public String getTagValueMatcher() {
            return tagValueMatcher;
        }

        public String[] getTagValues() {
            return tagValues;
        }
    }

    // Arrange the queries:
    // a) Group which can be executed on Cassandra
    // b) Group which requires Cassandra and in-memory
    // c) Group which is executed in memory by fetching metric definitions from Cassandra
    // Arrange each group to a sequence of most powerful query (such as exact query on Cassandra)

    // Between group b & c, fetch the metric definitions (of the current list)

    static class QueryOptimizer {

        enum RegExpOptimizer {OR_SINGLE_SEEK, NONE }

        public static final long GROUP_A_COST = 10;
        public static final long GROUP_A_OR_COST = 11;
        public static final long GROUP_B_COST = 50;
        public static final long GROUP_C_COST = 99;

        public static String IS_REGEXP = "^.*[]?+*{}()\\[^$|\\\\]+.*$|^[!].*";
        public static Pattern MATCH_REGEXP = Pattern.compile(IS_REGEXP);

        /**
         * Sorts the query language parameters based on their query cost.
         *
         * @param tagsQuery User's TagQL
         * @return TreeMap with Long key indicating query cost (lower is better)
         */
        public static Map<Long, List<Query>> reOrderTagsQuery(Map<String, String> tagsQuery,
                                                                                  boolean enableACostQueries) {
            Map<Long, List<Query>> costSortedMap = new TreeMap<>();
            costSortedMap.put(GROUP_A_COST, new ArrayList<>());
            costSortedMap.put(GROUP_A_OR_COST, new ArrayList<>());
            costSortedMap.put(GROUP_B_COST, new ArrayList<>());
            costSortedMap.put(GROUP_C_COST, new ArrayList<>());

            for (Map.Entry<String, String> tagQuery : tagsQuery.entrySet()) {
                if(tagQuery.getKey().startsWith("!")) {
                    // In-memory sorted query, requires fetching all the definitions
                    costSortedMap.get(GROUP_C_COST).add(new Query(tagQuery.getKey(), tagQuery.getValue()));
                } else if(enableACostQueries && !isRegExp(tagQuery.getValue())) {
                    costSortedMap.get(GROUP_A_COST).add(new Query(tagQuery.getKey(), tagQuery.getValue()));
                } else {
                    RegExpOptimizer strategy = optimalStrategy(tagQuery.getValue());
                    switch(strategy) {
                        case OR_SINGLE_SEEK:
                            String[] queries = tagQuery.getValue().split("\\|");
                            costSortedMap.get(GROUP_A_OR_COST).add(new Query(tagQuery.getKey(), null, queries));
                            break;
                        default:
                            costSortedMap.get(GROUP_B_COST).add(new Query(tagQuery.getKey(), tagQuery.getValue()));
                    }
                }
            }

            return costSortedMap;
        }

        /**
         * Not a perfect matching way (regexp can't detect a regexp), but we'll try to match the most common queries to
         * tags processor.
         */
        public static boolean isRegExp(String tagValuesQuery) {
            return MATCH_REGEXP.matcher(tagValuesQuery).matches();
        }

        /**
         * Detect optimized Cassandra query strategy for regexp query
         */
        public static QueryOptimizer.RegExpOptimizer optimalStrategy(String tagValuesQuery) {
            String[] orParts = tagValuesQuery.split("\\|");
            if(orParts.length > 1) {
                // We have multiple smaller queries
                for (String orPart : orParts) {
                    if(isRegExp(orPart)) {
                        return RegExpOptimizer.NONE;
                    }
                }
                // All subqueries were single match queries
                return RegExpOptimizer.OR_SINGLE_SEEK;
            }

            return RegExpOptimizer.NONE;
        }
    }

    public Observable<MetricId<?>> findMetricIdentifiersWithFilters(String tenantId, MetricType<?> metricType,
                                                                    Map<String, String> tagsQueries) {

        Map<Long, List<Query>> costSortedMap =
                QueryOptimizer.reOrderTagsQuery(tagsQueries, enableACostQueries);

        List<Query> groupAEntries = costSortedMap.get(QueryOptimizer.GROUP_A_COST);
        List<Query> groupAOREntries = costSortedMap.get(QueryOptimizer.GROUP_A_OR_COST);
        List<Query> groupBEntries = costSortedMap.get(QueryOptimizer.GROUP_B_COST);
        List<Query> groupCEntries = costSortedMap.get(QueryOptimizer.GROUP_C_COST);

        Observable<MetricId<?>> groupMetrics = null;

        /*
        Potential candidates:

        Build a tree, only the first will fetch from Cassandra - rest will at most enrich

        A
        A+B+C
        A+B
        A+C
        B
        B+C
        C
         */
        if(!groupAEntries.isEmpty() || !groupAOREntries.isEmpty()) {
            // Option A
            if(!groupAEntries.isEmpty()) {
                groupMetrics = Observable.from(groupAEntries)
                        .flatMap(e -> dataAccess.findMetricsByTagNameValue(tenantId, e.getTagName(), e.getTagValueMatcher())
                                .compose(new TagsIndexRowTransformerFilter<>(metricType))
                                .compose(new ItemsToSetTransformer<>()))
                        .reduce((s1, s2) -> {
                            s1.retainAll(s2);
                            return s1;
                        })
                        .flatMap(Observable::from);
            }

            // It might be more costly to request n*findMetric from Cassandra, so we'll do extra Cassandra queries
            // for OR queries also - better worst case performance

            // Option AOR
            if(!groupAOREntries.isEmpty()) {
                Observable<MetricId<?>> groupAORMetrics = Observable.from(groupAOREntries)
                        .flatMap(e -> dataAccess.findMetricsByTagNameValue(tenantId, e.getTagName(), e.getTagValues())
                                .compose(new TagsIndexRowTransformerFilter<>(metricType))
                                .compose(new ItemsToSetTransformer<>())
                                .reduce((s1, s2) -> {
                                    s1.addAll(s2);
                                    return s1;
                                }))
                        .flatMap(Observable::from);

                if(groupMetrics == null) {
                    groupMetrics = groupAORMetrics;
                } else {
                    Observable<HashSet<MetricId<?>>> groupAPart = groupMetrics.toList().map(HashSet::new);
                    Observable<HashSet<MetricId<?>>> groupAORPart = groupAORMetrics.toList().map(HashSet::new);

                    groupMetrics = groupAPart.mergeWith(groupAORPart)
                            .reduce((s1, s2) -> {
                                s1.retainAll(s2);
                                return s1;
                            })
                            .flatMap(Observable::from);
                }
            }

            // Options A+B, A+B+C and A+C
            if(!groupBEntries.isEmpty() || !groupCEntries.isEmpty()) {
                Observable<Metric<?>> enrichedMetrics = groupMetrics
                        .flatMap(metricsService::findMetric);

                // Option +B (A+B and A+B+C)
                if(!groupBEntries.isEmpty()) {
                    enrichedMetrics = applyBFilters(enrichedMetrics, groupBEntries);
                }

                // Options +C (A+B+C and A+C)
                if(!groupCEntries.isEmpty()) {
                    enrichedMetrics = applyCFilters(enrichedMetrics, groupCEntries);
                }
                groupMetrics = enrichedMetrics.map(Metric::getMetricId);
            }

        } else if(!groupBEntries.isEmpty()) {
            // Options B
            // Fetch everything from the tagsQueries
            groupMetrics = Observable.from(groupBEntries)
                    .flatMap(e -> dataAccess.findMetricsByTagName(tenantId, e.getTagName())
                            .filter(tagValueFilter(e.getTagValueMatcher(), 3))
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

            // Option B+C
            if(!groupCEntries.isEmpty()) {
                groupMetrics = applyCFilters(groupMetrics.flatMap(metricsService::findMetric), groupCEntries)
                        .map(Metric::getMetricId);
            }
        } else {
            // Option C
            // Fetch all the available metrics for this tenant
            Observable<? extends MetricId<?>> tagsMetrics = dataAccess.findAllMetricsFromTagsIndex()
                    .compose(new TagsIndexRowTransformerFilter<>(metricType))
                    .filter(mId -> mId.getTenantId().equals(tenantId));

            Observable<MetricId<?>> dataMetrics = metricsService.findAllMetricIdentifiers()
                    .filter(m -> m.getTenantId().equals(tenantId))
                    .filter(metricTypeFilter(metricType));

            groupMetrics = applyCFilters(
                    Observable
                            .concat(tagsMetrics, dataMetrics)
                            .distinct()
                            .flatMap(metricsService::findMetric),
                    groupCEntries)
                    .map(Metric::getMetricId);
        }

        return groupMetrics;
    }

    private Observable<Metric<?>> applyBFilters(Observable<Metric<?>> metrics, List<Query> groupBEntries) {
        for (Query groupBQuery : groupBEntries) {
            metrics = metrics
                    .filter(tagValueFilter(groupBQuery.getTagValueMatcher(), groupBQuery.getTagName()));
        }
        return metrics;
    }

    private Observable<Metric<?>> applyCFilters(Observable<Metric<?>> metrics, List<Query> groupCEntries) {
        // TODO zipWith or something here instead of this monstrosity
        for (Query groupCQuery : groupCEntries) {
            metrics = metrics
                    .filter(tagNotExistsFilter(groupCQuery.getTagName().substring(1)));
        }
        return metrics;
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

    private Func1<Metric<?>, Boolean> tagValueFilter(String regexp, String tagName) {
        // If no such tagName -> no match
        return tMetric -> {
            String tagValue = tMetric.getTags().get(tagName);
            if(tagValue != null) {
                boolean positive = (!regexp.startsWith("!"));
                Pattern p = PatternUtil.filterPattern(regexp);
                return positive == p.matcher(tagValue).matches(); // XNOR
            }
            return false;
        };
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
