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
package org.hawkular.metrics.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Map;

import org.hawkular.metrics.schema.log.SchemaManagerLogger;
import org.hawkular.metrics.schema.log.SchemaManagerLogging;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;

/**
 * @author John Sanda
 * @author Heiko W. Rupp
 */
public class SchemaManager {
    private static final SchemaManagerLogger log = SchemaManagerLogging.getSchemaManagerLogger(SchemaManager.class);

    private final Session session;

    public SchemaManager(Session session) {
        this.session = session;
    }

    public void dropKeyspace(String keyspace) {
        log.infoDroppingKeyspace(keyspace);
        session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
    }

    public void createSchema(String keyspace) {
        log.infoCreatingSchemaForKeyspace(keyspace);

        ResultSet resultSet = session.execute("SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '" +
            keyspace + "'");
        if (!resultSet.isExhausted()) {
            log.infoSchemaAlreadyExists();
            return;
        }

        ImmutableMap<String, String> schemaVars = ImmutableMap.of("keyspace", keyspace);

        try (InputStream inputStream = getClass().getResourceAsStream("/schema.cql");
            InputStreamReader reader = new InputStreamReader(inputStream)) {
            String content = CharStreams.toString(reader);

            for (String cql : content.split("(?m)^-- #.*$")) {
                if (!cql.startsWith("--")) {
                    String updatedCQL = substituteVars(cql.trim(), schemaVars);
                    log.debugf("Executing CQL: %n%s%n", updatedCQL);
                    session.execute(updatedCQL);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Schema creation failed", e);
        }
    }

    private String substituteVars(String cql, Map<String, String> vars) throws IOException {
        try (TokenReplacingReader reader = new TokenReplacingReader(cql, vars);
            StringWriter writer = new StringWriter()) {
            char[] buffer = new char[32768];
            int cnt;
            while ((cnt = reader.read(buffer)) != -1) {
                writer.write(buffer, 0, cnt);
            }
            return writer.toString();
        }
    }
}

