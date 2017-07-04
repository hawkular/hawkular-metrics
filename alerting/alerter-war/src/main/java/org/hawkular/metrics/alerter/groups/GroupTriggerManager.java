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
package org.hawkular.metrics.alerter.groups;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

import org.hawkular.alerts.api.json.GroupMemberInfo;
import org.hawkular.alerts.api.model.condition.CompareCondition;
import org.hawkular.alerts.api.model.condition.Condition;
import org.hawkular.alerts.api.model.trigger.Trigger;
import org.hawkular.alerts.api.services.DefinitionsEvent;
import org.hawkular.alerts.api.services.DefinitionsListener;
import org.hawkular.alerts.api.services.DefinitionsService;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.Metric;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.jboss.logging.Logger;

/**
 * Another feature of the Metrics Alerter is an ability to manage a Metrics Group Trigger.  A Metrics Group Trigger
 * is an hAlerting group trigger that has a member trigger for each metric is a set scoped by a metrics tag query.
 * The concept is similar to data-driven triggers but because metrics does not lend itself to the required
 * source+dataId approach used by data-driven triggers, we use this approach.
 * <p>
 * We interact with hAlerting to pull properly tagged group triggers.  We set up a job for each to query for
 * metrics, and maintain member triggers for each.
 * </p><p>
 * Each Metrics Group Trigger has several tags to guide its handling:
 * </p><pre>
 *   "HawkularMetrics" : "GroupTrigger"       // Required to be processed as Metrics Group Trigger
 *   "DataIds"         : DataId1..DataIdN     // The DataIds used in the trigger conditions, required
 *   DataId1           : tag query            // Metrics tag query for dataId1 metrics
 *     ...
 *   DataIdN           : tag query            // Metrics tag query for dataIdN metrics
 *   "SourceBy"        : TagName1..TagNameN   // *see below*
 * </pre>
 * <h4>SourceBy</h4>
 * The SourceBy tag specifies the metric tag names used to determine the member trigger population.  As an example,
 * consider a group trigger with one CompareCondition:  HeapUsed > 80% HeapMax.  We want to compare HeapUsed to HeapMax
 * but of course each comparison should be done on metrics from the same machine.  We don't want to compare all
 * HeapUsed to all HeapMax for every machine in the data center.  We'd want a group trigger tagged something like this:
 * <pre>
 *   "HawkularMetrics" : "GroupTrigger"
 *   "DataIds"         : "HeapUsed,HeapMax"
 *   "HeapUsed"        : name = "HeapUsed"
 *   "HeapMax"         : name = "HeapMax"
 *   "SourceBy"        : Machine
 * </pre>
 * This would result in one member trigger per machine, for machine names common to HeapUsed and HeapMax metrics. So,
 * if we had the following metrics in the database, tagged as specified:
 * <pre>
 *   /machine0/HeapUsed    {name=HeapUsed, Machine=machine0}
 *   /machine0/HeapMax     {name=HeapMax, Machine=machine0}
 *   /machine1/HeapUsed    {name=HeapUsed, Machine=machine1}
 *   /machine1/HeapMax     {name=HeapMax, Machine=machine1}
 *   /machine2/HeapUsed    {name=HeapUsed}
 *   /machine2/HeapMax     {name=HeapMax}
 *   /machine3/HeapUsed    {name=HeapUsed, Machine=machine3}
 *   /machine4/HeapMax     {name=HeapMax, Machine=machine4}

 * </pre>
 * Then we would get two member triggers, one each testing:
 * <pre>
 *   /machine0/HeapUsed < 80% /machine0/HeapMax
 *   /machine1/HeapUsed < 80% /machine1/HeapMax
 * </pre><p>
 * We would not have a 3rd member trigger because machine2 metrics don't have the necessary tags, machine3 does not
 * have the HeapMax metric, and machine4 does not have the HeapUsed metric.
 * </p><p>
 * SourceBy is required, but can be set to '*' to generate member triggers for all value combinations (not recommended
 * when multiple dataIds are involved).
 * </p>
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
@Startup
@Singleton
public class GroupTriggerManager {
    private final Logger log = Logger.getLogger(GroupTriggerManager.class);

    private static final String TAG_DATA_IDS = "DataIds";
    private static final String TAG_GROUP_TRIGGER = "HawkularMetrics";
    private static final String TAG_GROUP_TRIGGER_VALUE = "GroupTrigger";
    private static final String TAG_SOURCE_BY = "SourceBy";

    private static final Integer THREAD_POOL_SIZE;
    private static final String THREAD_POOL_SIZE_DEFAULT = "20";
    private static final String THREAD_POOL_SIZE_PROPERTY = "hawkular-metrics.alerter.mgt.pool-size";
    private static final Integer JOB_PERIOD;
    private static final String JOB_PERIOD_DEFAULT = String.valueOf(60 * 60); // 1 hour
    private static final String JOB_PERIOD_PROPERTY = "hawkular-metrics.alerter.mgt.job-period-seconds";

    static {
        int v;
        try {
            v = Integer.valueOf(System.getProperty(THREAD_POOL_SIZE_PROPERTY, THREAD_POOL_SIZE_DEFAULT));
        } catch (Exception e) {
            v = Integer.valueOf(THREAD_POOL_SIZE_DEFAULT);
        }
        THREAD_POOL_SIZE = v;

        try {
            v = Integer.valueOf(System.getProperty(JOB_PERIOD_PROPERTY, JOB_PERIOD_DEFAULT));
        } catch (Exception e) {
            v = Integer.valueOf(JOB_PERIOD_DEFAULT);
        }
        JOB_PERIOD = v;
    }

    ScheduledThreadPoolExecutor mgtExecutor;
    Map<Trigger, ScheduledFuture<?>> mgtFutures = new HashMap<>();

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
        log.infof("Starting Hawkular Metrics Group Trigger Manager, distributed=%s", distributed);

        mgtExecutor = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);

        refresh();

        if (null == definitionsListener) {
            log.info("Registering Trigger UPDATE/REMOVE listener");
            definitionsListener = new DefinitionsListener() {
                @Override
                public void onChange(List<DefinitionsEvent> events) {
                    if (coordinator) {
                        log.debugf("Refreshing due to change events %s", events);
                        refresh();
                    }
                }
            };
            definitions.registerListener(definitionsListener, DefinitionsEvent.Type.TRIGGER_CONDITION_CHANGE,
                    DefinitionsEvent.Type.TRIGGER_REMOVE);
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
        log.infof("Stopping Hawkular Metrics Group Trigger Manager, distributed=%s", distributed);

        if (null != mgtFutures) {
            mgtFutures.values().forEach(f -> f.cancel(true));
        }
        if (null != mgtExecutor) {
            mgtExecutor.shutdown();
            mgtExecutor = null;
        }
    }

    public void cancel(Trigger mgt) {
        ScheduledFuture<?> f = mgtFutures.get(mgt);
        if (null != f) {
            log.debugf("Canceling Hawkular Metrics Group Trigger Job for %s", mgt);
            try {
                f.cancel(true);
                mgtFutures.remove(mgt);
            } catch (Exception e) {
                log.warnf("Failed to cancel Hawkular Metrics Group Trigger Job for %s: %s", mgt, e.getMessage());
            }
        }
    }

    private synchronized void refresh() {
        try {
            // get all of the metrics group triggers (tagged for hawkular metrics)
            Collection<Trigger> taggedMgts = definitions.getAllTriggersByTag(
                    TAG_GROUP_TRIGGER,
                    TAG_GROUP_TRIGGER_VALUE);
            log.debugf("Refreshing [%s] Metrics Group Triggers!", taggedMgts.size());

            // generate valid set of metrics group trigger ids
            Set<Trigger> mgts = taggedMgts.stream()
                    .filter(t -> {
                        if (!t.isGroup()) {
                            log.warnf("Tagged as Metrics Group Trigger is not a group trigger, skipping %s", t);
                            return false;
                        }
                        return true;
                    })
                    .collect(Collectors.toSet());

            // for each metrics group trigger run a job to manage its member trigger set
            for (Trigger mgt : mgts) {
                if (mgtFutures.containsKey(mgt)) {
                    log.debugf("Already running job for MGT %s", mgt);

                } else {
                    try {
                        // start the job.
                        log.infof("Adding MGT runner for %s with job period %d seconds", mgt, JOB_PERIOD);

                        MGTRunner runner = new MGTRunner(metrics, definitions, mgt);
                        mgtFutures.put(
                                mgt,
                                mgtExecutor.scheduleAtFixedRate(runner, 0L, JOB_PERIOD, TimeUnit.SECONDS));
                    } catch (Exception e) {
                        log.errorf("Failed to schedule metrics group trigger %s: %s ", mgt, e.getMessage());
                    }
                }
            }

            // cancel any job for an mgt not in the current set
            Set<Trigger> doomedJobs = mgtFutures.keySet().stream()
                    .filter(k -> !mgts.contains(k))
                    .collect(Collectors.toSet());
            doomedJobs.stream().forEach(k -> {
                log.infof("Canceling obsolete MGT runner for %s", k);
                cancel(k);
            });

        } catch (Exception e) {
            log.error("Failed to refresh Metrics Group Triggers.", e);
        }
    }

    static class MGTRunner implements Runnable {
        private final Logger log = Logger.getLogger(GroupTriggerManager.MGTRunner.class);

        private MetricsService metricsService;
        private DefinitionsService definitionsService;
        private Trigger metricsGroupTrigger;
        private Integer dataIdMapHash;

        public MGTRunner(MetricsService metrics, DefinitionsService definitionsService, Trigger metricsGroupTrigger) {
            super();
            this.metricsService = metrics;
            this.definitionsService = definitionsService;
            this.metricsGroupTrigger = metricsGroupTrigger;
        }

        @Override
        public void run() {
            try {
                log.debugf("Running job for MGT %s", metricsGroupTrigger);

                // fetch the trigger to make sure it still exists and is tagged appropriately
                Trigger mgt = definitionsService.getTrigger(metricsGroupTrigger.getTenantId(),
                        metricsGroupTrigger.getId());

                if (null == mgt) {
                    log.warnf("Metrics Group Trigger not found. Skipping %s", mgt);
                    return;
                }
                if (!mgt.isGroup()) {
                    log.warnf("Metrics Group Trigger is not a group trigger, skipping %s", mgt);
                    return;
                }
                Map<String, String> tags = mgt.getTags();
                if (isEmpty(tags.getOrDefault(TAG_DATA_IDS, ""))) {
                    log.warnf("Required Metrics Group Trigger tag [%s] missing or null, skipping %s",
                            TAG_DATA_IDS, mgt);
                    return;
                }
                if (isEmpty(tags.getOrDefault(TAG_SOURCE_BY, ""))) {
                    log.warnf("Required Metrics Group Trigger tag [%s] missing or null, skipping %s",
                            TAG_SOURCE_BY, mgt);
                    return;
                }
                // check to make sure each declared dataId has a tag of the same name
                Set<String> dataIds = Arrays.asList(tags.get(TAG_DATA_IDS).split(",")).stream()
                        .map(s -> s.trim())
                        .collect(Collectors.toSet());
                if (dataIds.stream().anyMatch(s -> tags.getOrDefault(s, "").trim().isEmpty())) {
                    log.warnf(
                            "Metrics Group Trigger dataId tag missing or invalid. DataIds %s, Tags %s, skipping %s",
                            dataIds, tags, mgt);
                    return;
                }
                // get dataId set from conditions and ensure they are the same as declared
                Set<String> conditionDataIds = new HashSet<>();
                try {
                    Collection<Condition> conditions = definitionsService.getTriggerConditions(mgt.getTenantId(),
                            mgt.getId(), null);
                    conditionDataIds
                            .addAll(conditions.stream().map(c -> c.getDataId()).collect(Collectors.toSet()));
                    conditionDataIds.addAll(conditions.stream()
                            .filter(c -> c instanceof CompareCondition)
                            .map(c -> ((CompareCondition) c).getDataId()).collect(Collectors.toSet()));
                } catch (Exception e) {
                    log.error("Failed to fetch Conditions when refreshing Metrics Group Trigger " + mgt, e);
                    return;
                }
                if (!dataIds.equals(conditionDataIds)) {
                    log.warnf("Metrics Group Trigger dataId mismatch. In Tag: %s, In Conditions: %s. Skipping %s",
                            dataIds, conditionDataIds, mgt);
                    return;
                }

                // fetch the metric set for each dataId
                Map<String, Set<Metric<?>>> dataIdMap = new HashMap<>();
                for (String dataId : dataIds) {
                    String tagQuery = tags.get(dataId);
                    List<Metric<Object>> dataIdMetrics = metricsService
                            .findMetricIdentifiersWithFilters(mgt.getTenantId(), null, tagQuery)
                            .flatMap(metricsService::findMetric)
                            .toList()
                            .toBlocking().firstOrDefault(Collections.emptyList());
                    dataIdMap.put(dataId, new HashSet<>(dataIdMetrics));
                }

                // if there is no change in the dataIdMap since the last run we can stop here because the
                // member set remains the same.
                Integer hash = dataIdMap.hashCode();
                if (hash.equals(dataIdMapHash)) {
                    log.debugf("Metrics Group Trigger has no changes to member set, skipping %s", mgt);
                    return;
                }
                dataIdMapHash = hash;

                // Generate the member triggers.
                // build a map of source->map<dataId, list<metricName>>
                Map<List<String>, Map<String, List<String>>> sourceMap = generateSourceMap(tags.get(TAG_SOURCE_BY),
                        dataIdMap);

                // using the sourceMap generate the memberTriggers. Every tuple for every source is a valid member
                // as long as it has a metricName for each dataId.
                Set<GroupMemberInfo> members = generateMembers(mgt, dataIds, sourceMap);

                // now fetch the existing members and synchronize
                Collection<Trigger> existingMembers = definitionsService.getMemberTriggers(mgt.getTenantId(),
                        mgt.getId(), true);
                Set<String> memberIds = members.stream()
                        .map(m -> m.getMemberId())
                        .collect(Collectors.toSet());
                Set<String> existingMemberIds = existingMembers.stream()
                        .map(t -> t.getId())
                        .collect(Collectors.toSet());
                log.tracef("members: %s", members);
                log.tracef("memberIds        : %s", memberIds);
                log.tracef("existingMemberIds: %s", existingMemberIds);
                for (GroupMemberInfo member : members) {
                    if (existingMemberIds.contains(member.getMemberId())) {
                        log.tracef("Member already exists, skipping %s", member);
                        continue;
                    }
                    try {
                        log.debugf("Adding Member %s", member);
                        definitionsService.addMemberTrigger(mgt.getTenantId(), mgt.getId(),
                                member.getMemberId(),
                                member.getMemberName(),
                                member.getMemberDescription(),
                                member.getMemberContext(),
                                member.getMemberTags(),
                                member.getDataIdMap());
                    } catch (Exception e) {
                        log.warnf("Failed creating member %s: %s", member, e.getMessage());
                    }
                }
                existingMemberIds.removeAll(memberIds);
                for (String doomedMemberId : existingMemberIds) {
                    try {
                        log.debugf("Removing trigger %s", doomedMemberId);
                        definitionsService.removeTrigger(mgt.getTenantId(), doomedMemberId);
                    } catch (Exception e) {
                        log.warnf("Failed to delete member %s: %s", doomedMemberId, e.getMessage());
                    }
                }
            } catch (Exception e) {
                log.error("Failed to fetch Triggers for scheduling metrics conditions.", e);
            }
        }

        // generate the dataId tuples for the member trigger set. Each member will use dataIds with the same
        // sourceBy tag values. For example:
        //   member1:
        //     HeapUsed /dc1/m1/HeapUsed   tags:{datacenter:dc1,machine:m1}
        //     HeapMax  /dc1/m1/HeapMax    tags:{datacenter:dc1,machine:m1}
        //   member2:
        //     HeapUsed /dc1/m2/HeapUsed   tags:{datacenter:dc1,machine:m2}
        //     HeapMax  /dc1/m2/HeapMax    tags:{datacenter:dc1,machine:m2}
        //
        // To do this, first build a map of source->map<dataId, list<metricName>> and then
        // use it to generate the member tuples
        static Map<List<String>, Map<String, List<String>>> generateSourceMap(String sourceByTag,
                Map<String, Set<Metric<?>>> dataIdMap) {

            // sourceBy is the ordered list of tag names used to group the members
            List<String> sourceBy = Arrays.asList(sourceByTag.split(",")).stream()
                    .map(s -> s.trim())
                    .collect(Collectors.toList());
            boolean isStarSourceBy = ("*".equals(sourceBy.get(0)));
            List<String> starSource = isStarSourceBy ? Arrays.asList("*") : null;

            // sourceMap is the result, mapping a source to the dataId substitutions for that source
            // In the example above, for:
            //   sourceBy=["datacenter","machine"].
            // One mapping in sourceMap would be:
            //   {["dc1","m1"] => {"HeapUsed" => ["/dc1/m1/HeapUsed"],"HeapMax" => ["/dc1/m1/HeapMax"]}}
            Map<List<String>, Map<String, List<String>>> sourceMap = new HashMap<>();

            // dataIdMap maps the declared dataIds to the metrics in the db resulting from the tagQuery. loop
            // through the entries and construct the sourceMap
            for (Entry<String, Set<Metric<?>>> entry : dataIdMap.entrySet()) {
                String dataId = entry.getKey();
                Set<Metric<?>> metrics = entry.getValue();

                for (Metric<?> metric : metrics) {
                    // determine the source for this metric
                    List<String> source = isStarSourceBy ? starSource : sourceBy.stream()
                            .map(s -> metric.getTags().get(s))
                            .filter(s -> null != s)
                            .collect(Collectors.toList());
                    // ignore metrics that don't have a tag for each sourceBy entry
                    if (source.size() < sourceBy.size()) {
                        continue;
                    }

                    Map<String, List<String>> metricsMap = sourceMap.get(source);
                    if (null == metricsMap) {
                        metricsMap = new HashMap<>();
                    }
                    List<String> metricNames = metricsMap.get(dataId);
                    if (null == metricNames) {
                        metricNames = new ArrayList<>();
                    }
                    metricNames.add(metric.getId());
                    metricsMap.put(dataId, metricNames);
                    sourceMap.put(source, metricsMap);
                }
            }

            return sourceMap;
        }

        static Set<GroupMemberInfo> generateMembers(Trigger mgt, Set<String> dataIds,
                Map<List<String>, Map<String, List<String>>> sourceMap) {

            // Because member triggers inherit the group triggers tags, we override the HawkularMetrics tag to
            // reflect that this is a member trigger and not an MGT (preventing it from being fetched on refresh)
            Map<String, String> memberTags = new HashMap<>();
            memberTags.put("HawkularMetrics", "MemberTrigger");

            Set<GroupMemberInfo> members = new HashSet<>();
            for (Entry<List<String>, Map<String, List<String>>> sourceMapEntry : sourceMap.entrySet()) {
                List<String> source = sourceMapEntry.getKey();
                Map<String, List<String>> metricsMap = sourceMapEntry.getValue();

                // If we can't form a tuple because 1 or more required dataIds have no metrics, then continue
                if (metricsMap.size() < dataIds.size()) {
                    Logger log = Logger.getLogger(GroupTriggerManager.MGTRunner.class);
                    log.warnf("No MGT members: source: %s tags: %s dataIds: %s", source, metricsMap.keySet(),
                            dataIds);
                    continue;
                }

                List<Map<String, String>> memberDataIdMaps = new ArrayList<>();

                for (Entry<String, List<String>> metricsMapEntry : metricsMap.entrySet()) {
                    String dataId = metricsMapEntry.getKey();
                    List<String> metricNames = metricsMapEntry.getValue();

                    if (memberDataIdMaps.isEmpty()) {
                        for (String metricName : metricNames) {
                            Map<String, String> memberDataIdMap = new HashMap<>();
                            memberDataIdMap.put(dataId, metricName);
                            memberDataIdMaps.add(memberDataIdMap);
                        }
                    } else {
                        List<Map<String, String>> prevMemberDataIdMaps = memberDataIdMaps;
                        memberDataIdMaps = new ArrayList<>();
                        for (Map<String, String> prevMemberDataIdMap : prevMemberDataIdMaps) {
                            for (String metricName : metricNames) {
                                Map<String, String> memberDataIdMap = new HashMap<>();
                                memberDataIdMap.putAll(prevMemberDataIdMap);
                                memberDataIdMap.put(dataId, metricName);
                                memberDataIdMaps.add(memberDataIdMap);
                            }
                        }
                    }
                }

                Map<String, String> memberContext = new HashMap<>();
                memberContext.put("source", source.toString());

                for (Map<String, String> memberDataIdMap : memberDataIdMaps) {
                    // use the memberDataIdMap hashcode as the member trigger id. We'll use this id to
                    // determine whether the member already exists.
                    GroupMemberInfo member = new GroupMemberInfo(mgt.getId(),
                            String.valueOf(memberDataIdMap.hashCode()), null, null, memberContext,
                            memberTags, memberDataIdMap);
                    members.add(member);
                }
            }
            return members;
        }

        private boolean isEmpty(String s) {
            return null == s || s.trim().isEmpty();
        }
    }
}
