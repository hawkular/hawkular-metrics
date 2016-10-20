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
package org.hawkular.metrics.core.service.log;

import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import org.hawkular.metrics.model.MetricType;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Core library logging messages.
 *
 * @author Thomas Segismont
 */
@MessageLogger(projectCode = "HAWKMETRICS")
@ValidIdRange(min = 100000, max = 109999)
public interface CoreLogger extends BasicLogger {

    @LogMessage(level = INFO)
    @Message(id = 100001, value = "Using a key space of '%s'")
    void infoKeyspaceUsed(String keyspace);

    @LogMessage(level = WARN)
    @Message(id = 100002, value = "Uncaught exception on scheduled thread [%s]")
    void errorUncaughtExceptionOnScheduledThread(String threadName, @Cause Throwable t);

    @LogMessage(level = WARN)
    @Message(id = 100003, value = "Tenant creation failed")
    void warnTenantCreationFailed(@Cause Throwable t);

    @LogMessage(level = WARN)
    @Message(id = 100004, value = "Failed to delete tenants bucket [%d]")
    void warnFailedToDeleteTenantBucket(Long bucket, @Cause Throwable t);

    @LogMessage(level = WARN)
    @Message(id = 100005, value = "Failed to load data retentions for {tenantId: %s, metricType: %s}")
    void warnDataRetentionLoadingFailure(String tenantId, MetricType<?> metricType, @Cause Throwable t);

    @LogMessage(level = WARN)
    @Message(id = 100006, value = "There was an error persisting rates for {tenant= %s, start= %d, end= %d}")
    void warnFailedToPersistRates(String tenantId, Long start, Long end, @Cause Throwable t);

    @LogMessage(level = INFO)
    @Message(id = 100007, value = "Using default data retention of %d seconds")
    void infoDefaultDataRetention(int defaultTTL);

    @LogMessage(level = INFO)
    @Message(id = 100008, value = "Using max size of %d for string metrics")
    void infoMaxSizeStringMetrics(int maxStringSize);

    @LogMessage(level = INFO)
    @Message(id = 100009, value = "Using max number of retries %d and max retry delay of %d ms for inserting data " +
            "points")
    void infoInsertRetryConfig(int maxRetries, long maxRetryDelay);
}
