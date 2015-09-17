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

package org.hawkular.metrics.clients.ptrans.log;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * PTrans logging messages.
 *
 * @author Thomas Segismont
 */
@MessageLogger(projectCode = "HAWKMETRICS")
@ValidIdRange(min = 500000, max = 509999)
public interface PTransLogger extends BasicLogger {

    @LogMessage(level = INFO)
    @Message(id = 500001, value = "Starting ptrans...")
    void infoStarting();

    @LogMessage(level = ERROR)
    @Message(id = 500002, value = "Exception on startup")
    void errorStartupProblem(@Cause Exception e);

    @LogMessage(level = INFO)
    @Message(id = 500003, value = "%s listening on %s %s")
    void infoServerListening(String serverType, String protocol, Object address);

    @LogMessage(level = INFO)
    @Message(id = 500004, value = "ptrans started")
    void infoStarted();

    @LogMessage(level = INFO)
    @Message(id = 500005, value = "Stopping ptrans...")
    void infoStopping();

    @LogMessage(level = INFO)
    @Message(id = 500006, value = "ptrans stopped")
    void infoStopped();
}
