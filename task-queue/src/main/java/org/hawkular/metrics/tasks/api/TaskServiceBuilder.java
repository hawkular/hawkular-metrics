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
package org.hawkular.metrics.tasks.api;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Session;
import org.hawkular.metrics.tasks.impl.LeaseService;
import org.hawkular.metrics.tasks.impl.Queries;
import org.hawkular.metrics.tasks.impl.TaskServiceImpl;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;

/**
 * A builder for creating and configuring a {@link TaskService} instance.
 *
 * @author jsanda
 */
public class TaskServiceBuilder {

    private TimeUnit timeUnit = TimeUnit.MINUTES;

    private Session session;

    private List<TaskType> taskTypes;

    /**
     * The time unit determines a couple things. First, it determines the frequency for scheduling jobs. If
     * {@link TimeUnit#MINUTES} is used for instance, then jobs are scheduled every minute. In this context, a job
     * refers to finding and executing tasks in the queue for a particular time slice, which brings up the second
     * thing that <code>timeUnit</code> determines - time slice interval. Time slices are set along fixed intervals,
     * e.g., 13:00, 13:01, 13:02, etc.
     */
    public TaskServiceBuilder withTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
        return this;
    }

    /**
     * The session object to use for querying Cassandra.
     */
    public TaskServiceBuilder withSession(Session session) {
        this.session = session;
        return this;
    }

    /**
     * Specifies the {@link TaskType types} of tasks that the {@link TaskService} will execute. The order is significant
     * as it determines the order in which tasks are executed.
     */
    public TaskServiceBuilder withTaskTypes(List<TaskType> taskTypes) {
        this.taskTypes = taskTypes;
        return this;
    }

    public TaskService build() {
        Queries queries = new Queries(session);
        RxSession rxSession = new RxSessionImpl(session);
        LeaseService leaseService = new LeaseService(rxSession, queries);
        TaskServiceImpl taskService = new TaskServiceImpl(rxSession, queries, leaseService, taskTypes);
        taskService.setTimeUnit(timeUnit);

        return taskService;
    }

}
