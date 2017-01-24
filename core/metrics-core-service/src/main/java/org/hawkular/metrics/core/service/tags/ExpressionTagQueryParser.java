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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.PatternUtil;
import org.hawkular.metrics.core.service.tags.parser.TagQueryBaseListener;
import org.hawkular.metrics.core.service.tags.parser.TagQueryLexer;
import org.hawkular.metrics.core.service.tags.parser.TagQueryParser;
import org.hawkular.metrics.core.service.tags.parser.TagQueryParser.ArrayContext;
import org.hawkular.metrics.core.service.tags.parser.TagQueryParser.ObjectContext;
import org.hawkular.metrics.core.service.tags.parser.TagQueryParser.PairContext;
import org.hawkular.metrics.core.service.tags.parser.TagQueryParser.ValueContext;
import org.hawkular.metrics.core.service.transformers.ItemsToSetTransformer;
import org.hawkular.metrics.core.service.transformers.TagsIndexRowTransformer;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;

import rx.Observable;

/**
 * @author Stefan Negrea
 */
public class ExpressionTagQueryParser {

    private DataAccess dataAccess;
    private MetricsService metricsService;

    public ExpressionTagQueryParser(DataAccess dataAccess, MetricsService metricsService) {
        this.dataAccess = dataAccess;
        this.metricsService = metricsService;
    }

    public <T> Observable<Metric<T>> parse(String tenantId, MetricType<T> metricType, String expression) {
        ANTLRInputStream input = new ANTLRInputStream(expression);
        TagQueryLexer tql = new TagQueryLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(tql);
        TagQueryParser parser = new TagQueryParser(tokens);
        ParseTree parseTree = parser.tagquery();

        StorageTagQueryListener<T> listener = new StorageTagQueryListener<>(tenantId, metricType);
        ParseTreeWalker.DEFAULT.walk(listener, parseTree);

        return listener.getResult();
    }

    private class StorageTagQueryListener<T> extends TagQueryBaseListener {

        private Map<Integer, List<String>> arrays = new HashMap<>();
        private Map<Integer, List<Observable<MetricId<T>>>> observables = new HashMap<>();
        private String tenantId;
        private MetricType<T> metricType;

        public StorageTagQueryListener(String tenantId, MetricType<T> metricType) {
            this.tenantId = tenantId;
            this.metricType = metricType;
        }

        public Observable<Metric<T>> getResult() {
            if (observables.size() == 1) {
                return observables.values().iterator().next().get(0)
                    .flatMap(metricsService::findMetric);
            }

            return Observable.empty();
        }

        @Override
        public void exitPair(PairContext ctx) {
            String tagName = ctx.key().getText();

            Observable<MetricId<T>> result = null;
            int dataIndex = 3;

            if (ctx.array_operator() != null) {
                // extra ' characters are already removed by the array listener
                List<String> valueArray = arrays.get(ctx.array().getText().hashCode());
                List<Pattern> patterns = new ArrayList<>(valueArray.size());
                valueArray.forEach(tagValue -> patterns.add(PatternUtil.filterPattern(tagValue)));
                boolean positive = ctx.array_operator().NOT() == null;

                result = dataAccess.findMetricsByTagName(this.tenantId, tagName)
                        .filter(r -> {
                            for (Pattern p : patterns) {
                                if (positive && p.matcher(r.getString(dataIndex)).matches()) {
                                    return true;
                                } else if (!positive && p.matcher(r.getString(dataIndex)).matches()) {
                                    return false;
                                }
                            }

                            return !positive;
                        })
                        .compose(new TagsIndexRowTransformer<>(metricType))
                        .compose(new ItemsToSetTransformer<>())
                        .reduce((s1, s2) -> {
                            s1.addAll(s2);
                            return s1;
                        })
                        .flatMap(Observable::from);
            } else if (ctx.boolean_operator() != null) {
                String tagValue = null;

                if (ctx.value().COMPLEXTEXT() != null) {
                    tagValue = ctx.value().COMPLEXTEXT().getText();
                    tagValue = tagValue.substring(1, tagValue.length() - 1);
                } else if (ctx.value().SIMPLETEXT() != null) {
                    tagValue = ctx.value().SIMPLETEXT().getText();
                }

                Pattern p = PatternUtil.filterPattern(tagValue);
                boolean positive = ctx.boolean_operator().EQUAL() != null;

                result = dataAccess.findMetricsByTagName(this.tenantId, tagName)
                        .filter(r -> positive == p.matcher(r.getString(dataIndex)).matches())
                        .compose(new TagsIndexRowTransformer<>(metricType))
                        .compose(new ItemsToSetTransformer<>())
                        .reduce((s1, s2) -> {
                            s1.addAll(s2);
                            return s1;
                        })
                        .flatMap(Observable::from);
            }

            pushObservable(ctx.getText().hashCode(), result);
        }

        @Override
        public void exitObject(ObjectContext ctx) {
            if (ctx.logical_operator() != null) {
                Observable<MetricId<T>> leftObservable = popObservable(ctx.object(0).getText().hashCode());
                Observable<MetricId<T>> rightObservable = popObservable(ctx.object(1).getText().hashCode());

                observables.remove(ctx.object(0).getText().hashCode());
                observables.remove(ctx.object(1).getText().hashCode());

                Observable<MetricId<T>> result = leftObservable.concatWith(rightObservable);

                if (ctx.logical_operator().AND() != null) {
                    //group by metric and then use one element from the groups with two elements
                    //if a group has two elements it is in both sets, hence AND
                    result = result
                            .groupBy(m -> m)
                            .flatMap(s -> s.skip(1).take(1));
                } else if (ctx.logical_operator().OR() != null) {
                    result = result.distinct();
                }

                pushObservable(ctx.getText().hashCode(), result);
            } else {
                if (ctx.object(0) != null && ctx.object(0).getText().hashCode() != ctx.getText().hashCode()) {
                    Observable<MetricId<T>> expressionObservable = popObservable(ctx.object(0).getText().hashCode());
                    observables.remove(ctx.object(0).getText().hashCode());
                    pushObservable(ctx.getText().hashCode(), expressionObservable);
                }
            }
        };

        @Override
        public void enterArray(ArrayContext ctx) {
            List<String> arrayContext = new ArrayList<String>();
            for (ValueContext node : ctx.value()) {
                if (node.COMPLEXTEXT() != null) {
                    String text = node.COMPLEXTEXT().getText();
                    arrayContext.add(text.substring(1, text.length() - 1));
                } else if (node.SIMPLETEXT() != null) {
                    String text = node.SIMPLETEXT().getText();
                    arrayContext.add(text);
                }

            }
            arrays.put(ctx.getText().hashCode(), arrayContext);
        }

        private void pushObservable(Integer hashCode, Observable<MetricId<T>> observable) {
            List<Observable<MetricId<T>>> hashObservables = observables.get(hashCode);
            if (hashObservables != null) {
                hashObservables.add(observable);
            } else {
                hashObservables = new ArrayList<Observable<MetricId<T>>>();
                hashObservables.add(observable);
                observables.put(hashCode, hashObservables);
            }
        }

        private Observable<MetricId<T>> popObservable(Integer hashCode) {
            List<Observable<MetricId<T>>> hashObservables = observables.get(hashCode);

            if (hashObservables == null || hashObservables.isEmpty()) {
                return null;
            }

            Observable<MetricId<T>> observable = hashObservables.remove(0);

            if (hashObservables.isEmpty()) {
                observables.remove(hashCode);
            }

            return observable;
        }
    }
}
