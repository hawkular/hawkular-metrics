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
package org.hawkular.metrics.schema;

import static java.util.Collections.singletonList;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.Manifest;

import org.cassalog.core.Cassalog;
import org.cassalog.core.CassalogBuilder;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableMap;

/**
 * @author jsanda
 */
public class SchemaService {

    public void run(Session session, String keyspace, boolean resetDB) {


        CassalogBuilder builder = new CassalogBuilder();
        Cassalog cassalog = builder.withKeyspace(keyspace).withSession(session).build();
        Map<String, ?> vars  = ImmutableMap.of(
                "keyspace", keyspace,
                "reset", resetDB,
                "session", session
        );
        // TODO Add logic to determine the version tags we need to pass
        // For now, I am just hard coding the version tag, but a more robust solution would be
        // to calculate the tags using the current version stored in the system_settings table
        // and the new version which we can extract from any of our JAR manifest files.
        List<String> tags = singletonList("0.15.x");
        URI script = getScript();
        cassalog.execute(script, tags, vars);

        session.execute("INSERT INTO " + keyspace + ".system_settings (key, value) VALUES " +
                "('hawkular.metrics.version', '" + getNewHawkularMetricsVersion() + "')");
    }

    private URI getScript() {
        try {
            return getClass().getResource("/org/hawkular/schema/cassalog.groovy").toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to load schema change script", e);
        }
    }

    private boolean systemSettingsTableExists(Session session, String keyspace) {
        Statement statement = new SimpleStatement("SELECT * FROM system.schema_columnfamilies WHERE " +
                "keyspace_name = '" + keyspace + "' AND columnfamily_name = 'system_settings'");
        ResultSet resultSet = session.execute(statement);
        return !resultSet.isExhausted();
    }

    private String getCurrentHawkularMetricsVersion(Session session, String keyspace) {
        Statement statement = new SimpleStatement("SELECT value FROM " + keyspace + ".system_settings WHERE " +
                "key = 'hawkular.metrics.version'");
        ResultSet resultSet = session.execute(statement);
        if (resultSet.isExhausted()) {
            return null;
        }
        return resultSet.all().get(0).getString(0);
    }

    private String getNewHawkularMetricsVersion() {
        try {
            Enumeration<URL> resources = getClass().getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                Manifest manifest = new Manifest(resource.openStream());
                String vendorId = manifest.getMainAttributes().getValue("Implementation-Vendor-Id");
                if (vendorId != null && vendorId.equals("org.hawkular.metrics")) {
                    return manifest.getMainAttributes().getValue("Implementation-Version");
                }
            }
            throw new RuntimeException("Unable to determine implementation version for Hawkular Metrics");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
