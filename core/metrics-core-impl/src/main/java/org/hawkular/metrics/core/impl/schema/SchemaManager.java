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
package org.hawkular.metrics.core.impl.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;

import org.hawkular.metrics.core.impl.util.TokenReplacingReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author John Sanda
 * @author Heiko W. Rupp
 */
public class SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

    private Session session;

    public SchemaManager(Session session) {
        this.session = session;
    }

    public void dropKeyspace(String keyspace) {
        logger.info("Dropping keyspace " + keyspace);
        session.execute("DROP KEYSPACE " + keyspace);
    }

    public void createSchema(String keyspace) throws IOException {
        logger.info("Creating schema for keyspace " + keyspace);

        ResultSet resultSet = session.execute("SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '" +
            keyspace + "'");
        if (!resultSet.isExhausted()) {
            logger.info("Schema already exist. Skipping schema creation.");
            return;
        }

        ImmutableMap<String, String> schemaVars = ImmutableMap.of("keyspace", keyspace);

        try (InputStream inputStream = getClass().getResourceAsStream("/schema.cql");
            InputStreamReader reader = new InputStreamReader(inputStream)) {
            String content = CharStreams.toString(reader);

            for (String cql : content.split("(?m)^-- #.*$")) {
                if (!cql.startsWith("--")) {
                    String updatedCQL = substituteVars(cql.trim(), schemaVars);
                    logger.info("Executing CQL:\n" + updatedCQL + "\n");
                    session.execute(updatedCQL);
                }
            }
        }
    }

    private String substituteVars(String cql, Map<String, String> vars) {
        try (TokenReplacingReader reader = new TokenReplacingReader(cql, vars);
            StringWriter writer = new StringWriter()) {
            char[] buffer = new char[32768];
            int cnt;
            while ((cnt = reader.read(buffer)) != -1) {
                writer.write(buffer, 0, cnt);
            }
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException("Failed to perform variable substition on CQL", e);
        }
    }

}

