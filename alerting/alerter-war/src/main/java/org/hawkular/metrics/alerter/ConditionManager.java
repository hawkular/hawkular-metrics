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
package org.hawkular.metrics.alerter;

import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER_RATE;
import static org.hawkular.metrics.model.MetricType.GAUGE_RATE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

import org.hawkular.alerts.api.model.condition.Condition;
import org.hawkular.alerts.api.model.condition.ExternalCondition;
import org.hawkular.alerts.api.model.event.Event;
import org.hawkular.alerts.api.model.trigger.Trigger;
import org.hawkular.alerts.api.services.AlertsService;
import org.hawkular.alerts.api.services.DefinitionsEvent;
import org.hawkular.alerts.api.services.DefinitionsEvent.Type;
import org.hawkular.alerts.api.services.DefinitionsListener;
import org.hawkular.alerts.api.services.DefinitionsService;
import org.hawkular.metrics.alerter.ConditionExpression.EvalType;
import org.hawkular.metrics.alerter.ConditionExpression.Query;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.BucketPoint;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.exception.RuntimeApiError;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.jboss.logging.Logger;

import rx.Observable;

/**
 * Manages the Metrics expression evaluations and interacts with the Alerts system.  Sets up fixed rate thread
 * jobs to back each ExternalCondition.
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
@Startup
@Singleton
public class ConditionManager {
    private final Logger log = Logger.getLogger(ConditionManager.class);

    private static final String TAG_EXTERNAL_CONDITION_NAME = "HawkularMetrics";
    private static final String TAG_EXTERNAL_CONDITION_VALUE = "MetricsCondition";

    private static final Integer THREAD_POOL_SIZE;
    private static final String THREAD_POOL_SIZE_DEFAULT = "20";
    private static final String THREAD_POOL_SIZE_PROPERTY = "hawkular-metrics.alerter.condition-pool-size";

    static {
        int v;
        try {
            v = Integer.valueOf(System.getProperty(THREAD_POOL_SIZE_PROPERTY, THREAD_POOL_SIZE_DEFAULT));
        } catch (Exception e) {
            v = Integer.valueOf(THREAD_POOL_SIZE_DEFAULT);
        }
        THREAD_POOL_SIZE = v;
    }

    ScheduledThreadPoolExecutor expressionExecutor;
    Map<ExternalCondition, ScheduledFuture<?>> expressionFutures = new HashMap<>();

    /**
     * Indicate if the deployment is on a clustering scenario and if so if this is the coordinator node
     */
    private boolean distributed = false;
    private boolean coordinator = false;
    private TopologyChangeListener topologyChangeListener = null;
    private DefinitionsListener definitionsListener = null;

    @Inject
    private MetricsService metrics;

    @Inject
    private DefinitionsService definitions;

    @Inject
    private AlertsService alerts;

    /**
     * Access to the manager of the caches used for the partition services.  This is used to inspect cluster
     * topology and ensure we only run the external alerter on one node (the coordinator). Otherwise
     * each node would execute the same work, resulting in duplicate alerts.
     */
    @Resource(lookup = "java:jboss/infinispan/container/hawkular-alerts")
    private EmbeddedCacheManager cacheManager;

    @PostConstruct
    public void init() {
        // Cache manager has an active transport (i.e. jgroups) when is configured on distributed mode
        distributed = cacheManager.getTransport() != null;

        if (distributed) {
            topologyChangeListener = new TopologyChangeListener();
            cacheManager.addListener(topologyChangeListener);
        }

        processTopologyChange();
    }

    // When a node is joining/leaving the cluster the coordinator node could change
    @Listener
    public class TopologyChangeListener {
        @ViewChanged
        public void onTopologyChange(ViewChangedEvent cacheEvent) {
            processTopologyChange();
        }
    }

    private void processTopologyChange() {
        boolean currentCoordinator = coordinator;
        coordinator = distributed ? cacheManager.isCoordinator() : true;

        if (coordinator && !currentCoordinator) {
            start();

        } else if (!coordinator && currentCoordinator) {
            stop();
        }
    }

    public void start() {
        log.infof("Starting Hawkular Metrics External Alerter, distributed=%s", distributed);

        expressionExecutor = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);

        refresh();

        if (null == definitionsListener) {
            log.info("Registering Trigger UPDATE/REMOVE listener");
            definitionsListener = new DefinitionsListener() {
                @Override
                public void onChange(Set<DefinitionsEvent> event) {
                    if (coordinator) {
                        refresh();
                    }
                }
            };
            definitions.registerListener(definitionsListener, DefinitionsEvent.Type.TRIGGER_UPDATE,
                    Type.TRIGGER_REMOVE);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (coordinator) {
            stop();
        }

        if (distributed) {
            cacheManager.removeListener(topologyChangeListener);
            cacheManager.stop();
        }
    }

    public void stop() {
        log.infof("Stopping Hawkular Metrics External Alerter, distributed=%s", distributed);

        if (null != expressionFutures) {
            expressionFutures.values().forEach(f -> f.cancel(true));
        }
        if (null != expressionExecutor) {
            expressionExecutor.shutdown();
            expressionExecutor = null;
        }
    }

    private void refresh() {
        log.debug("Refreshing External Metrics Triggers!");
        try {
            Set<ExternalCondition> activeConditions = new HashSet<>();

            // get all of the triggers tagged for hawkular metrics
            Collection<Trigger> triggers = definitions.getAllTriggersByTag(
                    TAG_EXTERNAL_CONDITION_NAME,
                    TAG_EXTERNAL_CONDITION_VALUE);
            log.debugf("Found [%s] External Metrics Triggers!", triggers.size());

            // for each trigger look for Metrics Conditions and start running them
            Collection<Condition> conditions = null;
            for (Trigger trigger : triggers) {
                try {
                    if (trigger.isEnabled()) {
                        conditions = definitions.getTriggerConditions(trigger.getTenantId(), trigger.getId(), null);
                        log.debugf("Checking [%s] Conditions for enabled trigger [%s]!", conditions.size(),
                                trigger.getName());
                    }
                } catch (Exception e) {
                    log.error("Failed to fetch Conditions when scheduling metrics conditions for " + trigger, e);
                    continue;
                }
                if (null == conditions) {
                    continue;
                }
                for (Condition condition : conditions) {
                    if (condition instanceof ExternalCondition) {
                        ExternalCondition externalCondition = (ExternalCondition) condition;
                        if (TAG_EXTERNAL_CONDITION_NAME.equals(externalCondition.getAlerterId())) {
                            log.debugf("Found Metrics ExternalCondition %s", externalCondition);
                            activeConditions.add(externalCondition);
                            if (expressionFutures.containsKey(externalCondition)) {
                                log.debugf("Skipping, already evaluating %s", externalCondition);

                            } else {
                                try {
                                    // start the job. TODO: Do we need a delay for any reason?
                                    log.debugf("Adding runner for %s", externalCondition);

                                    ConditionExpression expression = ConditionExpression
                                            .toObject(externalCondition.getExpression());
                                    ExpressionRunner runner = new ExpressionRunner(metrics, alerts, trigger,
                                            externalCondition, expression);
                                    expressionFutures.put(
                                            externalCondition,
                                            expressionExecutor.scheduleAtFixedRate(runner, 0L,
                                                    expression.getFrequencyDuration().getValue(),
                                                    expression.getFrequencyDuration().getTimeUnit()));
                                } catch (Exception e) {
                                    log.error("Failed to schedule expression for metrics condition "
                                            + externalCondition, e);
                                }
                            }
                        }
                    }
                }
            }

            // cancel obsolete expressions
            Set<ExternalCondition> temp = new HashSet<>();
            for (Map.Entry<ExternalCondition, ScheduledFuture<?>> me : expressionFutures.entrySet()) {
                ExternalCondition ec = me.getKey();
                if (!activeConditions.contains(ec)) {
                    log.debugf("Canceling evaluation of obsolete External Metric Condition %s", ec);

                    me.getValue().cancel(true);
                    temp.add(ec);
                }
            }
            expressionFutures.keySet().removeAll(temp);
            temp.clear();

        } catch (Exception e) {
            log.error("Failed to fetch Triggers for scheduling metrics conditions.", e);
        }
    }

    private static class ExpressionRunner implements Runnable {
        private final Logger log = Logger.getLogger(ConditionManager.ExpressionRunner.class);

        private MetricsService metricsService;
        private AlertsService alertsService;
        private Trigger trigger;
        private ExternalCondition externalCondition;
        private ConditionExpression expression;

        public ExpressionRunner(MetricsService metrics, AlertsService alerts, Trigger trigger,
                ExternalCondition externalCondition,
                ConditionExpression expression) {
            super();
            this.metricsService = metrics;
            this.alertsService = alerts;
            this.trigger = trigger;
            this.externalCondition = externalCondition;
            this.expression = expression;
        }

        @Override
        public void run() {
            if (EvalType.ALL == expression.getEvalType()) {
                runOnAll();

            } else {
                runOnEach();
            }
        }

        @SuppressWarnings("unchecked")
        private void runOnAll() {
            try {
                long now = System.currentTimeMillis();
                Map<String, BucketPoint> queryResults = new HashMap<>();
                for (Query q : expression.getQueries()) {
                    MetricType<?> type = q.getMetricsType();
                    boolean isAvail = AVAILABILITY == type;
                    boolean isRate = COUNTER_RATE == type || GAUGE_RATE == type;
                    if (isRate) {
                        type = (COUNTER_RATE == type) ? MetricType.COUNTER : MetricType.GAUGE;
                    }
                    List<String> metrics = isEmpty(q.getMetrics()) ? null : new ArrayList<>(q.getMetrics());
                    String tags = q.getTags();
                    long end = now - q.getMetricsOffset().toMillis();
                    long start = end - q.getMetricsDuration().toMillis();
                    List<Percentile> percentiles = q.getMetricsPercentiles();
                    List<BucketPoint> result;

                    if (isAvail) {
                        result = findMetricsByNameOrTags(trigger.getTenantId(), metrics, tags, AVAILABILITY)
                                .toList()
                                .flatMap(metricIds -> {
                                    if (metricIds.size() != 1) {
                                        String err = "Only one Availability metric currently supported. Found ["
                                                + metricIds.size() + "] using metrics=" + metrics + " tags=" + tags;
                                        throw new IllegalArgumentException(err);
                                    }
                                    return metricsService.findAvailabilityStats(metricIds.get(0), start, end,
                                            Buckets.fromCount(start, end, 1));
                                })
                                .toBlocking()
                                .firstOrDefault(Collections.EMPTY_LIST);

                    } else {
                        // Note, stacked is always false as we are already limiting to a single bucket
                        result = findMetricsByNameOrTags(trigger.getTenantId(), metrics, tags,
                                (MetricType<Double>) type)
                                        .toList()
                                        .flatMap(metricIds -> metricsService.findNumericStats(metricIds, start, end,
                                                Buckets.fromCount(start, end, 1), percentiles, false, isRate))
                                        .toBlocking()
                                        .firstOrDefault(Collections.EMPTY_LIST);
                    }
                    if (result.size() != 1) {
                        throw new IllegalStateException(
                                "Failed to retrieve proper data " + result + " for query [%s]" + q.getName());
                    }

                    if (log.isDebugEnabled()) {
                        log.debugf("Performing Query [%s]", q.getName());
                        log.debugf("        Type : %s (rate=%s)", type, isRate);
                        log.debugf("     Metrics : %s", metrics);
                        log.debugf("        Tags : %s", tags);
                        log.debugf("      Bucket : %s", Buckets.fromCount(start, end, 1));
                        log.debugf(" Percentiles : %s", percentiles);
                    }

                    queryResults.put(q.getName(), result.get(0));
                }

                log.debugf("Query Results: %s", queryResults);

                ConditionEvaluator evaluator = expression.getEvaluator();
                evaluate("", queryResults, evaluator);

            } catch (Throwable t) {
                if (log.isDebugEnabled()) {
                    t.printStackTrace();
                }
                log.warnf("Failed data fetch for %s: %s", expression, t.getMessage());
            }
        }

        @SuppressWarnings("unchecked")
        private void runOnEach() {
            try {
                long now = System.currentTimeMillis();
                Map<String, Map<String, List<? extends BucketPoint>>> queryResults = new HashMap<>();
                for (Query q : expression.getQueries()) {
                    MetricType<?> type = q.getMetricsType();
                    boolean isAvail = AVAILABILITY == type;
                    boolean isRate = COUNTER_RATE == type || GAUGE_RATE == type;
                    if (isRate) {
                        type = (COUNTER_RATE == type) ? MetricType.COUNTER : MetricType.GAUGE;
                    }
                    List<String> metrics = isEmpty(q.getMetrics()) ? null : new ArrayList<>(q.getMetrics());
                    String tags = q.getTags();
                    long end = now - q.getMetricsOffset().toMillis();
                    long start = end - q.getMetricsDuration().toMillis();
                    List<Percentile> percentiles = q.getMetricsPercentiles();
                    Observable<Map<String, List<? extends BucketPoint>>> oResult;
                    Map<String, List<? extends BucketPoint>> result;

                    if (isAvail) {
                        oResult = findMetricsByNameOrTags(trigger.getTenantId(), metrics, tags, AVAILABILITY)
                                .flatMap(metricId -> metricsService.findAvailabilityStats(metricId, start, end,
                                        Buckets.fromCount(start, end, 1))
                                        .map(bucketPoints -> Collections.singletonMap(metricId.getName(),
                                                bucketPoints)))
                                .collect(HashMap::new, (rMap, statsMap) -> rMap.putAll(statsMap));

                    } else {
                        // Note, stacked is always false as we are already limiting to a single bucket
                        oResult = findMetricsByNameOrTags(trigger.getTenantId(),
                                metrics, tags, (MetricType<Double>) type)
                                        .flatMap(metricId -> metricsService
                                                .findNumericStats(Collections.singletonList(metricId),
                                                        start, end, Buckets.fromCount(start, end, 1), percentiles,
                                                        false, isRate)
                                                .map(bucketPoints -> Collections.singletonMap(metricId.getName(),
                                                        bucketPoints)))
                                        .collect(HashMap::new, (rMap, statsMap) -> rMap.putAll(statsMap));
                    }

                    result = oResult.toBlocking().firstOrDefault(Collections.EMPTY_MAP);

                    if (log.isDebugEnabled()) {
                        log.debugf("Performing Query [%s]", q.getName());
                        log.debugf("        Type : %s (rate=%s)", type, isRate);
                        log.debugf("     Metrics : %s", metrics);
                        log.debugf("        Tags : %s", tags);
                        log.debugf("      Bucket : %s", Buckets.fromCount(start, end, 1));
                        log.debugf(" Percentiles : %s", percentiles);
                    }

                    queryResults.put(q.getName(), result);
                }

                log.debugf("Query Results: %s", queryResults);

                ConditionEvaluator evaluator = expression.getEvaluator();
                evaluateEach("", queryResults, evaluator);

            } catch (Throwable t) {
                if (log.isDebugEnabled()) {
                    t.printStackTrace();
                }
                log.warnf("Failed data fetch for %s: %s", expression, t.getMessage());
            }
        }

        private void evaluateEach(String target, Map<String, Map<String, List<? extends BucketPoint>>> queryResults,
                ConditionEvaluator evaluator) {

            // get the Set of metrics common to all of the queries
            Set<String> metrics = null;
            for (String queryName : queryResults.keySet()) {
                if (null == metrics) {
                    metrics = new HashSet<>(queryResults.get(queryName).keySet());
                    continue;
                }
                metrics.retainAll(queryResults.get(queryName).keySet());
            }

            // evaluate each
            Map<String, BucketPoint> metricQueryResults = new HashMap<>();
            Map<String, String> preparedCondition = new HashMap<>();
            for (String metric : metrics) {
                metricQueryResults.clear();
                preparedCondition.clear();

                for (String queryName : queryResults.keySet()) {
                    metricQueryResults.put(queryName, queryResults.get(queryName).get(metric).get(0));
                }

                try {
                    preparedCondition = evaluator.prepare(metricQueryResults);
                } catch (Exception e) {
                    log.debugf("Could not prepare evaluation, Skipping due to [%s]", e.getMessage());
                }

                if (evaluator.evaluate()) {
                    try {
                        Event externalEvent = new Event(externalCondition.getTenantId(), UUID.randomUUID().toString(),
                                System.currentTimeMillis(), externalCondition.getDataId(),
                                TAG_EXTERNAL_CONDITION_VALUE,
                                preparedCondition.toString(),
                                Collections.singletonMap("metricName", metric), null);
                        log.debugf("Sending External Condition Event to alerting: %s", externalEvent);
                        alertsService.sendEvents(Collections.singleton(externalEvent));

                    } catch (Exception e) {
                        log.error("Failed to send external [EACH] event to alerting system.", e);
                    }
                }
            }
        }

        private void evaluate(String target, Map<String, BucketPoint> queryResults, ConditionEvaluator evaluator) {
            Map<String, String> preparedCondition = null;
            try {
                preparedCondition = evaluator.prepare(queryResults);
            } catch (Exception e) {
                log.debugf("Could not prepare evaluation, Skipping due to [%s]", e.getMessage());
            }

            if (evaluator.evaluate()) {
                try {
                    Event externalEvent = new Event(externalCondition.getTenantId(), UUID.randomUUID().toString(),
                            System.currentTimeMillis(), externalCondition.getDataId(),
                            TAG_EXTERNAL_CONDITION_VALUE,
                            preparedCondition.toString());
                    log.debugf("Sending External Condition Event to Alerting %s", externalEvent);
                    alertsService.sendEvents(Collections.singleton(externalEvent));

                } catch (Exception e) {
                    log.error("Failed to send external [ALL] event to alerts system.", e);
                }
            }
        }

        private boolean isEmpty(Collection<?> c) {
            return null == c || c.isEmpty();
        }

        private boolean isEmpty(String s) {
            return null == s || s.trim().isEmpty();
        }

        private <T> Observable<MetricId<T>> findMetricsByNameOrTags(String tenantId, List<String> metricNames,
                String tags, MetricType<T> type) {

            if (isEmpty(metricNames) && isEmpty(tags)) {
                return Observable.error(new RuntimeApiError("Either metrics or tags parameter must be used"));
            }

            if (!isEmpty(metricNames)) {
                if (!isEmpty(tags)) {
                    return Observable.error(new RuntimeApiError("Cannot use both the metrics and tags parameters"));
                }

                return Observable.from(metricNames)
                        .map(id -> new MetricId<>(tenantId, type, id));
            }

            // Tags case
            return metricsService.findMetricsWithFilters(tenantId, type, tags)
                    .map(Metric::getMetricId);
        }
    }

}
