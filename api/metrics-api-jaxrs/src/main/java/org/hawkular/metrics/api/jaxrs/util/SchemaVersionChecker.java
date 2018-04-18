/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.api.jaxrs.util;

import org.hawkular.metrics.schema.SchemaService;
import org.jboss.logging.Logger;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
/**
 * @author jsanda
 */
public class SchemaVersionChecker {

    private static final Logger logger = Logger.getLogger(SchemaVersionChecker.class);

    public void waitForSchemaUpdates(Session session, String keyspace, long delay, int maxRetries)
            throws InterruptedException {
        String configId = "org.hawkular.metrics";
        String configName = "schema-version";
        int retries = 0;
        PreparedStatement getVersion = null;
        SchemaService schemaService = new SchemaService(session, keyspace);
        String version = schemaService.getSchemaVersion();

        while (maxRetries > retries) {
            try {
                if (getVersion == null) {
                    getVersion = session.prepare("SELECT value FROM " + keyspace + ".sys_config " +
                            "WHERE config_id = ? AND name = ?");
                }
                ResultSet resultSet = session.execute(getVersion.bind(configId, configName));
                if (!resultSet.isExhausted()) {
                    String schemaVersion = resultSet.one().getString(0);
                    if(version.equals(schemaVersion)) {
                        logger.infof("Hawkular Metrics schema is up to date at version %s", version);
                        return;
                    } else {
                        logger.infof("Hawkular Metrics schema version does not match," +
                                        " should be %s, present: %s, Trying again in %d ms",
                                version, schemaVersion, delay);

                    }
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Version check failed", e);
                } else {
                    logger.infof("Version check failed: %s", e.getMessage());
                }
                logger.infof("Trying again in %d ms", delay);
            }
            retries++;
            Thread.sleep(delay);
        }

        throw new SchemaVersionCheckException("Version check unsuccessful after " + maxRetries + " attempts");
    }

}
