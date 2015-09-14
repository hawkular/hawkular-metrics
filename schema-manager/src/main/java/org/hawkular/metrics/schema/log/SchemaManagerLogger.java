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
package org.hawkular.metrics.schema.log;

import static org.jboss.logging.Logger.Level.INFO;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Schema manager logging messages.
 *
 * @author Thomas Segismont
 */
@MessageLogger(projectCode = "HAWKMETRICS")
@ValidIdRange(min = 300000, max = 309999)
public interface SchemaManagerLogger extends BasicLogger {

    @LogMessage(level = INFO)
    @Message(id = 300001, value = "Dropping keyspace %s")
    void infoDroppingKeyspace(String keyspace);

    @LogMessage(level = INFO)
    @Message(id = 300002, value = "Creating schema for keyspace %s")
    void infoCreatingSchemaForKeyspace(String keyspace);

    @LogMessage(level = INFO)
    @Message(id = 300003, value = "Schema already exists. Skipping schema creation")
    void infoSchemaAlreadyExists();
}
