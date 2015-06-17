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
package org.hawkular.metrics.tasks.impl;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * @author jsanda
 */
public class Queries {

    public PreparedStatement createLease;

    public PreparedStatement findLeases;

    public PreparedStatement acquireLease;

    public PreparedStatement renewLease;

    public PreparedStatement finishLease;

    public PreparedStatement deleteLeases;

    public PreparedStatement createTask;

    public PreparedStatement createTaskWithFailures;

    public PreparedStatement findTasks;

    public PreparedStatement deleteTasks;

    public Queries(Session session) {
        createLease = session.prepare(
            "INSERT INTO leases (time_slice, task_type, segment_offset) VALUES (?, ?, ?)");

        findLeases = session.prepare(
            "SELECT task_type, segment_offset, owner, finished FROM leases WHERE time_slice = ?");

        acquireLease = session.prepare(
            "UPDATE leases " +
            "USING TTL ? " +
            "SET owner = ? " +
            "WHERE time_slice = ? AND task_type = ? AND segment_offset = ? " +
            "IF owner = NULL");

        renewLease = session.prepare(
            "UPDATE leases " +
            "USING TTL ? " +
            "SET owner = ? " +
            "WHERE time_slice = ? AND task_type = ? AND segment_offset = ? " +
            "IF owner = ?");

        finishLease = session.prepare(
            "UPDATE leases " +
            "SET finished = true " +
            "WHERE time_slice = ? AND task_type = ? AND segment_offset = ? " +
            "IF owner = ?");

        deleteLeases = session.prepare("DELETE FROM leases WHERE time_slice = ?");

        createTask = session.prepare(
            "INSERT INTO task_queue (task_type, tenant_id, time_slice, segment, target, sources, interval, window) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        createTaskWithFailures = session.prepare(
            "INSERT INTO task_queue (task_type, tenant_id, time_slice, segment, target, sources, interval, window, " +
            "failed_time_slices) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

        findTasks = session.prepare(
            "SELECT tenant_id, target, sources, interval, window, failed_time_slices " +
            "FROM task_queue " +
            "WHERE task_type = ? AND time_slice = ? AND segment = ?");

        deleteTasks = session.prepare(
            "DELETE FROM task_queue WHERE task_type = ? AND time_slice = ? AND segment = ?");
    }

}
