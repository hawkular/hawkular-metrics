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

package org.hawkular.metrics.api.jaxrs.log;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.FATAL;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * REST interface logging messages.
 *
 * @author Thomas Segismont
 */
@MessageLogger(projectCode = "HAWKMETRICS")
@ValidIdRange(min = 200000, max = 209999)
public interface RestLogger extends BasicLogger {

    @LogMessage(level = INFO)
    @Message(id = 200001, value = "Hawkular Metrics starting")
    void infoAppStarting();

    @LogMessage(level = INFO)
    @Message(id = 200002, value = "Initializing metrics service")
    void infoInitializing();

    @LogMessage(level = WARN)
    @Message(id = 200003, value = "Could not connect to Cassandra cluster - assuming its not up yet: %s")
    void warnCouldNotConnectToCassandra(String msg);

    @LogMessage(level = WARN)
    @Message(id = 200004, value = "[%d] Retrying connecting to Cassandra cluster in [%d]s...")
    void warnRetryingConnectingToCassandra(Integer connectionAttempts, Long delay);

    @LogMessage(level = INFO)
    @Message(id = 200005, value = "Metrics service started")
    void infoServiceStarted();

    @LogMessage(level = FATAL)
    @Message(id = 200006, value = "An error occurred trying to connect to the Cassandra cluster")
    void fatalCannotConnectToCassandra(@Cause Exception e);

    @LogMessage(level = ERROR)
    @Message(id = 200007, value = "Could not shutdown the Metrics service instance")
    void errorCouldNotCloseServiceInstance(@Cause Exception e);

    @LogMessage(level = WARN)
    @Message(id = 200008, value = "Invalid CQL port %s, not a number. Will use a default of %s")
    void warnInvalidCqlPort(String port, String defaultPort);

    @LogMessage(level = ERROR)
    @Message(id = 200009, value = "Unexcepted exception while shutting down")
    void errorShutdownProblem(@Cause Exception e);

    @LogMessage(level = ERROR)
    @Message(id = 200010, value = "Failed to process request")
    void errorRequestProblem(@Cause Throwable t);

    @LogMessage(level = WARN)
    @Message(id = 200011, value = "Invalid value [%s] for default TTL. Will use a default of %s days")
    void warnInvalidDefaultTTL(String ttl, String defaultTTL);

    @LogMessage(level = WARN)
    @Message(id = 200012, value = "Invalid Cassandra Driver read timeout %s, not a number. Will use the default of %d ms.")
    void warnInvalidDriverReadTimeout(String found, int defaultValue);
}
