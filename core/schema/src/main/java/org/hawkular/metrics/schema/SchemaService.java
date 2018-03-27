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
package org.hawkular.metrics.schema;

import static java.util.Arrays.asList;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.cassalog.core.Cassalog;
import org.cassalog.core.CassalogBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.collect.ImmutableMap;

/**
 * @author jsanda
 */
public class SchemaService {

    private static Logger logger = LoggerFactory.getLogger(SchemaService.class);

    public void run(Session session, String keyspace, boolean resetDB) {
        run(session, keyspace, resetDB, 1);
    }

    public void run(Session session, String keyspace, boolean resetDB, int replicationFactor) {
        run(session, keyspace, resetDB, replicationFactor, false);
    }

    public void run(Session session, String keyspace, boolean resetDB, int replicationFactor, boolean updateVersion) {
        CassalogBuilder builder = new CassalogBuilder();
        Cassalog cassalog = builder.withKeyspace(keyspace).withSession(session).build();
        Map<String, ?> vars  = ImmutableMap.of(
                "keyspace", keyspace,
                "reset", resetDB,
                "session", session,
                "replicationFactor", replicationFactor,
                "logger", logger
        );
        // TODO Add logic to determine the version tags we need to pass
        // For now, I am just hard coding the version tag, but a more robust solution would be
        // to calculate the tags using the current version stored in the sys_config table
        // and the new version which we can extract from any of our JAR manifest files.
        List<String> tags = asList("0.15.x", "0.18.x", "0.19.x", "0.20.x", "0.21.x", "0.23.x", "0.26.x", "0.27.x",
                "0.30.x");
        URI script = getScript();
        cassalog.execute(script, tags, vars);

        if (updateVersion) {
            updateVersion(session, keyspace, 0, 0);
        }
    }

    public void updateVersion(Session session, String keyspace, long delay, int maxRetries) {
        doVersionUpdate(session, keyspace, delay, maxRetries);
    }

    private void doVersionUpdate(Session session, String keyspace, long delay, int maxRetries) {
        String configId = "org.hawkular.metrics";
        String configName = "version";
        String version = VersionUtil.getVersion();
        SimpleStatement updateVersion = new SimpleStatement(
                "INSERT INTO sys_config (config_id, name, value) VALUES (?, ?, ?)", configId, configName, version)
                .setKeyspace(keyspace);
        int retries = 0;

        while (true) {
            try {
                session.execute(updateVersion);
                logger.info("Updated system configuration to version {}", version);
                break;
            } catch (Exception e) {
                retries++;
                if (retries > maxRetries) {
                    throw new VersionUpdateException("Failed to update version in system configuration after " +
                            maxRetries + " attempts.", e);
                }
                logger.warn("Failed to update version in system configuration. Retrying in " + delay + " ms", e);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e1) {
                    throw new VersionUpdateException("Aborting version update due to interrupt", e1);
                }
            }
        }
    }

    private URI getScript() {
        try {
            return getClass().getResource("/org/hawkular/schema/cassalog.groovy").toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to load schema change script", e);
        }
    }

}
