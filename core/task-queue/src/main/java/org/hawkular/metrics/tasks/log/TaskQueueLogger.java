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
package org.hawkular.metrics.tasks.log;

import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import org.hawkular.metrics.tasks.api.Task2;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Task queue logging messages.
 *
 * @author Thomas Segismont
 */
@MessageLogger(projectCode = "HAWKMETRICS")
@ValidIdRange(min = 400000, max = 409999)
public interface TaskQueueLogger extends BasicLogger {

    @LogMessage(level = INFO)
    @Message(id = 400001, value = "Shutting down")
    void infoShutdown();

    @LogMessage(level = WARN)
    @Message(id = 400002, value = "Execution of %s failed")
    void warnTaskExecutionFailed(Task2 task, @Cause Exception e);

    @LogMessage(level = WARN)
    @Message(id = 400003, value = "There was an error observing tasks")
    void warnTasksObservationProblem(@Cause Throwable t);

    @LogMessage(level = WARN)
    @Message(id = 400004, value = "There was an error during post-task processing")
    void warnTaskPostProcessProblem(@Cause Throwable t);

    @LogMessage(level = WARN)
    @Message(id = 400005, value = "Interrupted waiting for task execution to complete")
    void warnInterruptionOnTaskCompleteWaiting(@Cause Exception e);

    @LogMessage(level = WARN)
    @Message(id = 400006, value = "There was an error observing leases")
    void warnLeasesObservationProblem(@Cause Throwable t);
}
