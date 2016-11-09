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
package org.hawkular.metrics.alerter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

import org.hawkular.alerts.api.model.condition.Condition;
import org.hawkular.alerts.api.model.condition.ExternalCondition;
import org.hawkular.alerts.api.model.data.Data;
import org.hawkular.alerts.api.model.trigger.Trigger;
import org.hawkular.alerts.api.services.AlertsService;
import org.hawkular.alerts.api.services.DefinitionsEvent;
import org.hawkular.alerts.api.services.DefinitionsEvent.Type;
import org.hawkular.alerts.api.services.DefinitionsListener;
import org.hawkular.alerts.api.services.DefinitionsService;
import org.hawkular.metrics.alerter.Expression.Func;
import org.hawkular.metrics.core.service.Aggregate;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.jboss.logging.Logger;

/**
 * Manages the Metrics expression evaluations and interacts with the Alerts system.
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
@Startup
@Singleton
public class ExpressionManager {
    private final Logger log = Logger.getLogger(ExpressionManager.class);

    private static final String TAG_NAME = "HawkularMetrics";
    private static final String TAG_VALUE = "MetricsCondition";

    private static final long DAY = 24L * 60L * 1000L;
    private static final long WEEK = 7L * DAY;

    private static final Integer THREAD_POOL_SIZE = 20;

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
                public void onChange(DefinitionsEvent event) {
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
            Collection<Trigger> triggers = definitions.getAllTriggersByTag(TAG_NAME, TAG_VALUE);
            log.debug("Found [" + triggers.size() + "] External Metrics Triggers!");

            // for each trigger look for Metrics Conditions and start running them
            Collection<Condition> conditions = null;
            for (Trigger trigger : triggers) {
                try {
                    if (trigger.isEnabled()) {
                        conditions = definitions.getTriggerConditions(trigger.getTenantId(), trigger.getId(), null);
                        log.debug("Checking [" + conditions.size() + "] Conditions for enabled trigger ["
                                + trigger.getName() + "]!");
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
                        if (TAG_NAME.equals(externalCondition.getAlerterId())) {
                            if (log.isDebugEnabled()) {
                                log.debug("Found Metrics ExternalCondition! " + externalCondition);
                            }
                            activeConditions.add(externalCondition);
                            if (expressionFutures.containsKey(externalCondition)) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Skipping, already evaluating: " + externalCondition);
                                }

                            } else {
                                try {
                                    // start the job. TODO: Do we need a delay for any reason?
                                    if (log.isDebugEnabled()) {
                                        log.debug("Adding runner for: " + externalCondition);
                                    }
                                    Expression expression = new Expression(externalCondition.getExpression());
                                    ExpressionRunner runner = new ExpressionRunner(metrics, alerts, trigger,
                                            externalCondition, expression);
                                    expressionFutures.put(
                                            externalCondition,
                                            expressionExecutor.scheduleAtFixedRate(runner, 0L,
                                                    expression.getInterval(), TimeUnit.MINUTES));
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
                    if (log.isDebugEnabled()) {
                        log.debug("Canceling evaluation of obsolete External Metric Condition " + ec);
                    }
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
        private final Logger log = Logger.getLogger(ExpressionManager.ExpressionRunner.class);

        private MetricsService metrics;
        private AlertsService alerts;
        private Trigger trigger;
        private ExternalCondition externalCondition;
        private Expression expression;

        public ExpressionRunner(MetricsService metrics, AlertsService alerts, Trigger trigger,
                ExternalCondition externalCondition,
                Expression expression) {
            super();
            this.metrics = metrics;
            this.alerts = alerts;
            this.trigger = trigger;
            this.externalCondition = externalCondition;
            this.expression = expression;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            try {
                Func func = expression.getFunc();
                String tenantId = trigger.getTenantId();
                long end = System.currentTimeMillis();
                long start = end - (expression.getPeriod() * 60000);

                log.debug("Running External Metrics Condition: " + expression);

                Double value = Double.NaN;
                switch (func) {
                    case avg: {
                        MetricId<Double> metricId = new MetricId<>(tenantId, MetricType.GAUGE, expression.getMetric());
                        value = metrics.findGaugeData(metricId, start, end, Aggregate.Average)
                                .toBlocking().last();
                        break;
                    }
                    case avgd: {
                        MetricId<Double> metricId = new MetricId<>(tenantId, MetricType.GAUGE, expression.getMetric());
                        Double avgToday = metrics
                                .findGaugeData(metricId, start, end, Aggregate.Average)
                                .toBlocking().last();
                        Double avgYesterday = metrics
                                .findGaugeData(metricId, (start - DAY), (end - DAY),
                                        Aggregate.Average)
                                .toBlocking().last();
                        value = ((avgToday - avgYesterday) / avgYesterday) * 100;
                        break;
                    }
                    case avgw: {
                        MetricId<Double> metricId = new MetricId<>(tenantId, MetricType.GAUGE, expression.getMetric());
                        Double avgToday = metrics
                                .findGaugeData(metricId, start, end, Aggregate.Average)
                                .toBlocking().last();
                        Double avgLastWeek = metrics
                                .findGaugeData(metricId, (start - WEEK), (end - WEEK),
                                        Aggregate.Average)
                                .toBlocking().last();
                        value = ((avgToday - avgLastWeek) / avgLastWeek) * 100;
                        break;
                    }
                    case range: {
                        MetricId<Double> metricId = new MetricId<>(tenantId, MetricType.GAUGE, expression.getMetric());
                        Iterator<Double> iterator = metrics.findGaugeData(metricId, start, end,
                                Aggregate.Min, Aggregate.Max)
                                .toBlocking().toIterable().iterator();
                        Double min = iterator.next();
                        Double max = iterator.next();
                        value = max - min;
                        break;
                    }
                    case rangep: {
                        MetricId<Double> metricId = new MetricId<>(tenantId, MetricType.GAUGE, expression.getMetric());
                        Iterator<Double> iterator = metrics.findGaugeData(metricId, start, end,
                                Aggregate.Min, Aggregate.Max, Aggregate.Average)
                                .toBlocking().toIterable().iterator();
                        Double min = iterator.next();
                        Double max = iterator.next();
                        Double avg = iterator.next();
                        value = (max - min) / avg;
                        break;
                    }
                    case max: {
                        MetricId<Double> metricId = new MetricId<>(tenantId, MetricType.GAUGE, expression.getMetric());
                        value = metrics.findGaugeData(metricId, start, end, Aggregate.Max)
                                .toBlocking().last();
                        break;
                    }
                    case min: {
                        MetricId<Double> metricId = new MetricId<>(tenantId, MetricType.GAUGE, expression.getMetric());
                        value = metrics.findGaugeData(metricId, start, end, Aggregate.Min)
                                .toBlocking().last();
                        break;
                    }
                    case heartbeat: {
                        MetricId<AvailabilityType> availId = new MetricId<>(tenantId, MetricType.AVAILABILITY,
                                expression.getMetric());
                        Iterator<DataPoint<AvailabilityType>> iterator = metrics
                                .findAvailabilityData(availId, start, end, false, -1, null)
                                .toBlocking().toIterable().iterator();
                        int upCount = 0;
                        while (iterator.hasNext()) {
                            if (AvailabilityType.UP == iterator.next().getValue()) {
                                ++upCount;
                            }
                        }
                        value = Double.valueOf(upCount);
                        break;
                    }
                    default: {
                        log.errorf("Unexpected Expression Function: %s", func);
                        break;
                    }
                }

                evaluate(value);

            } catch (Throwable t) {
                log.debug("Failed data fetch for " + expression + " : " + t.getMessage());
            }
        }

        public void evaluate(Double value) {
            if (value.isNaN()) {
                if (log.isDebugEnabled()) {
                    log.debug("NaN value, Ignoring External Metrics evaluation of " + expression);
                }
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Running External Metrics Evaluation: " + expression + " : " + value);
            }

            if (!expression.isTrue(value)) {
                return;
            }

            try {
                Data externalData = Data.forNumeric(externalCondition.getTenantId(), externalCondition.getDataId(),
                        System.currentTimeMillis(), value);
                if (log.isDebugEnabled()) {
                    log.debug("Sending External Condition Data to Alerts! " + externalData);
                }
                alerts.sendData(Collections.singleton(externalData));

            } catch (Exception e) {
                log.error("Failed to send external data to alerts system.", e);
            }
        }
    }

}
