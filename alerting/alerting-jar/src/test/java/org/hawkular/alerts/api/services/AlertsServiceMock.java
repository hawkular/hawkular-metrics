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
package org.hawkular.alerts.api.services;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;

import org.hawkular.alerts.api.model.condition.ConditionEval;
import org.hawkular.alerts.api.model.data.Data;
import org.hawkular.alerts.api.model.event.Alert;
import org.hawkular.alerts.api.model.event.Event;
import org.hawkular.alerts.api.model.paging.Page;
import org.hawkular.alerts.api.model.paging.Pager;
import org.hawkular.metrics.api.jaxrs.util.Eager;

@ApplicationScoped
@Eager
public class AlertsServiceMock implements AlertsService {

    public static Set<Data> DATA = new HashSet<>();

    @Override
    public void ackAlerts(String tenantId, Collection<String> alertIds, String ackBy, String ackNotes)
            throws Exception {

    }

    @Override
    public void addAlerts(Collection<Alert> alerts) throws Exception {

    }

    @Override
    public void addAlertTags(String tenantId, Collection<String> alertIds, Map<String, String> tags) throws Exception {

    }

    @Override
    public void addEvents(Collection<Event> events) throws Exception {

    }

    @Override
    public void addEventTags(String tenantId, Collection<String> eventIds, Map<String, String> tags) throws Exception {

    }

    @Override
    public void persistEvents(Collection<Event> events) throws Exception {

    }

    @Override
    public void addNote(String tenantId, String alertId, String user, String text) throws Exception {

    }

    @Override
    public int deleteAlerts(String tenantId, AlertsCriteria criteria) throws Exception {

        return 0;
    }

    @Override
    public int deleteEvents(String tenantId, EventsCriteria criteria) throws Exception {

        return 0;
    }

    @Override
    public Alert getAlert(String tenantId, String alertId, boolean thin) throws Exception {

        return null;
    }

    @Override
    public Page<Alert> getAlerts(String tenantId, AlertsCriteria criteria, Pager pager) throws Exception {

        return null;
    }

    @Override
    public Page<Alert> getAlerts(Set<String> arg0, AlertsCriteria arg1, Pager arg2) throws Exception {
        return null;
    }

    @Override
    public Event getEvent(String tenantId, String eventId, boolean thin) throws Exception {

        return null;
    }

    @Override
    public Page<Event> getEvents(String tenantId, EventsCriteria criteria, Pager pager) throws Exception {

        return null;
    }

    @Override
    public Page<Event> getEvents(Set<String> arg0, EventsCriteria arg1, Pager arg2) throws Exception {
        return null;
    }

    @Override
    public void removeAlertTags(String tenantId, Collection<String> alertIds, Collection<String> tags)
            throws Exception {

    }

    @Override
    public void removeEventTags(String tenantId, Collection<String> eventIds, Collection<String> tags)
            throws Exception {

    }

    @Override
    public void resolveAlerts(String tenantId, Collection<String> alertIds, String resolvedBy, String resolvedNotes,
            List<Set<ConditionEval>> resolvedEvalSets) throws Exception {

    }

    @Override
    public void resolveAlertsForTrigger(String tenantId, String triggerId, String resolvedBy, String resolvedNotes,
            List<Set<ConditionEval>> resolvedEvalSets) throws Exception {

    }

    @Override
    public void sendData(Collection<Data> data) throws Exception {
        DATA.addAll(data);
    }

    @Override
    public void sendEvents(Collection<Event> events) throws Exception {

    }

    @Override
    public void sendData(Collection<Data> data, boolean ignoreFiltering) throws Exception {
        DATA.addAll(data);
    }

    @Override
    public void sendEvents(Collection<Event> events, boolean ignoreFiltering) throws Exception {

    }
}
