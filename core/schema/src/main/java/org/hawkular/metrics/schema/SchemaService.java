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
import org.cassalog.core.ChangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

/**
 * @author jsanda
 */
public class SchemaService {

    private static Logger logger = LoggerFactory.getLogger(SchemaService.class);

    private PreparedStatement versionUpdateQuery;

    private Session session;

    private String keyspace;

    private Cassalog cassalog;

    public SchemaService(Session session, String keyspace) {
        this.keyspace = keyspace;
        this.session = session;
        CassalogBuilder builder = new CassalogBuilder();
        cassalog = builder.withKeyspace(keyspace).withSession(session).build();
    }

    public void run(boolean resetDB) {
        run(resetDB, 1);
    }

    public void run(boolean resetDB, int replicationFactor) {
        run(resetDB, replicationFactor, false);
    }

    public void run(boolean resetDB, int replicationFactor, boolean updateVersion) {

        URI script = getScript();
        Map<String, ?> vars = this.getVars(resetDB, replicationFactor);
        cassalog.execute(script, this.getTags(), vars);

        if (updateVersion) {
            updateVersion(0, 0);
        }
    }

    private PreparedStatement prepareUpdateStatementIfNeeded() {
        if(this.versionUpdateQuery == null) {
            versionUpdateQuery = session.prepare("INSERT INTO sys_config (config_id, name, value) VALUES (?, ?, ?)");
        }
        return this.versionUpdateQuery;
    }

    private Map<String, ?> getVars(boolean resetDB, int replicationFactor) {
        Map<String, ?> vars = ImmutableMap.of(
                "keyspace", keyspace,
                "reset", resetDB,
                "session", session,
                "replicationFactor", replicationFactor,
                "logger", logger
        );
        return vars;
    }


    private List<String> getTags() {
        // TODO Add logic to determine the version tags we need to pass
        // For now, I am just hard coding the version tag, but a more robust solution would be
        // to calculate the tags using the current version stored in the sys_config table
        // and the new version which we can extract from any of our JAR manifest files.
        List<String> tags = asList("0.15.x", "0.18.x", "0.19.x", "0.20.x", "0.21.x", "0.23.x", "0.26.x", "0.27.x",
                "0.30.x");
        return tags;
    }

    public void updateVersion(long delay, int maxRetries) {
        doVersionUpdate(delay, maxRetries);
    }

    public void updateSchemaVersionSession(long delay, int maxRetries) {
        doSchemaVersionUpdate(delay, maxRetries);
    }

    public String getSchemaVersion() {
        URI script = getScript();
        Map<String, ?> vars = this.getVars(false, 1);
        List<ChangeSet> changeSets = cassalog.load(script, this.getTags(), vars);
        if (changeSets.size() == 0) {
            throw new RuntimeException("Failed get schema version, there is no changes available.");
        }
        return changeSets.get(changeSets.size() - 1).getVersion();
    }

    private void doVersionUpdate(long delay, int maxRetries) {
        String version = VersionUtil.getVersion();
        writeVersion("version", version, delay, maxRetries);
    }

    private void doSchemaVersionUpdate(long delay, int maxRetries) {
        String version = this.getSchemaVersion();
        writeVersion("schema-version", version, delay, maxRetries);
    }

    private void writeVersion(String versionType, String version, long delay, int maxRetries) {
        String configId = "org.hawkular.metrics";
        BoundStatement boundStatement = prepareUpdateStatementIfNeeded().bind(configId, versionType, version);
        int retries = 0;
        session.execute("USE " + keyspace);
        while (true) {
            try {
                session.execute(boundStatement);
                logger.info("Updated system configuration to {} {}", versionType, version);
                break;
            } catch (Exception e) {
                retries++;
                if (retries > maxRetries) {
                    throw new VersionUpdateException("Failed to update" + versionType + "in system configuration " +
                            "after " + maxRetries + " attempts.", e);
                }
                logger.warn("Failed to update " + versionType + " in system configuration. Retrying in " + delay +
                        " ms", e);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e1) {
                    throw new VersionUpdateException("Aborting " + versionType + " update due to interrupt", e1);
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
