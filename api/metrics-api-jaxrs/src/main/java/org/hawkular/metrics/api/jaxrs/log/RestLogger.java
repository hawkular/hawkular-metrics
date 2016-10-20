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
    @Message(id = 200012, value = "Invalid value [%s] for max connections per host. Will use a default of %s")
    void warnInvalidMaxConnections(String maxConnectionsPerHost, String defaultMaxConnections);

    @LogMessage(level = WARN)
    @Message(id = 200013, value = "Invalid value [%s] for max requests per connection. Will use a default of %s")
    void warnInvalidMaxRequests(String maxRequestsPerConnection, String defaultMaxRequests);

    @LogMessage(level = WARN)
    @Message(id = 200014, value = "Invalid value [%s] for Cassandra driver request timeout. Will use default value " +
            "of %s")
    void warnInvalidRequestTimeout(String requestTimeout, String defaultRequestTimeout);

    @LogMessage(level = WARN)
    @Message(id = 200015, value = "Invalid value [%s] for Cassandra driver connection timeout. Will use default " +
            "value of %s")
    void warnInvalidConnectionTimeout(String connectionTimeout, String defaultConnectionTimeout);

    @LogMessage(level = WARN)
    @Message(id = 200016, value = "Invalid value [%s] for Cassandra driver schema refresh interval. Will use " +
            "default value of %s")
    void warnInvalidSchemaRefreshInterval(String schemaRefreshInterval, String defaultSchemaRefreshInterval);

    @LogMessage(level = WARN)
    @Message(id = 200017, value = "Invalid value [%s] for ingestion max retries. The ingestion configuration setting " +
            "will not be updated")
    void warnInvalidIngestMaxRetries(String maxRetries);

    @LogMessage(level = WARN)
    @Message(id = 200018, value = "Invalid value [%s] for ingestion max retry delay. The ingestion configuration " +
            "setting will not be updated")
    void warnInvalidIngestMaxRetryDelay(String maxRetries);
}
