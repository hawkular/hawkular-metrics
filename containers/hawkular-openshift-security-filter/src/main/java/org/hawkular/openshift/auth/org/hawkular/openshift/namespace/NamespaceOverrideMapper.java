/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.openshift.auth.org.hawkular.openshift.namespace;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.jboss.logging.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 */
public class NamespaceOverrideMapper {

    private static final Logger log = Logger.getLogger(NamespaceOverrideMapper.class);

    private Session session;

    private final String token;

    public Map<String,String> overrides;

    public NamespaceOverrideMapper(String token) {
        log.debug("Creating NamespaceOverrideMapper");

        this.token = token;

        boolean connected = false;
        do {
            try {
                session = createSession();
                connected = true;
            } catch (Exception e) {
                log.info("Could not connect to Cassandra. This could mean Cassandra is not yet up and running. Will try again. Error message: " + e.getLocalizedMessage());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        } while (!connected);
        log.debug("Connection to Cassandra establised");

        // ensure that the openshift_metrics keyspace exists
        ensureKeyspace("openshift_metrics");

        // if we already have a mappingTable stored in OpenShift, then use it.
        // Otherwise we need to generate the overwrite table.
        if (hasMappingTable()) {
            log.debug("Mapping table already exists in Cassandra. Using this table");
            this.overrides = getMappingTable();
        } else {
            log.debug("Mapping table does not exist in Cassandra. Creating Mapping");
            // get the current list of tenants in Hawkular Metrics
            Set<String> tenants = getTenants();

            // map the current tenants to the current projects
            Map<String, String> namespaceMapping = getNamespaceMapping(tenants);
            log.debug("Namespace mapping: " + namespaceMapping);

            this.overrides = namespaceMapping;

            // write this mapping table back to Cassandra. We don't want to ever recalculate this table
            log.debug("Writing the mapping table to Cassandra");
            writeMappingTable(this.overrides);
            log.debug("Mapping table stored in Cassandra");
        }
    }

    private Session createSession() {
        Cluster.Builder clusterBuilder = new Cluster.Builder();

        String nodes = System.getProperty("hawkular.metrics.cassandra.nodes", "hawkular-cassandra");
        Arrays.stream(nodes.split(",")).forEach(clusterBuilder::addContactPoint);

        if (System.getProperty("hawkular.metrics.cassandra.use-ssl") != null && !System.getProperty("hawkular.metrics.cassandra.use-ssl").equals("false")) {
            SSLOptions sslOptions = null;
            try {
                String[] defaultCipherSuites = {"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"};
                sslOptions = JdkSSLOptions.builder().withSSLContext(SSLContext.getDefault())
                        .withCipherSuites(defaultCipherSuites).build();
                clusterBuilder.withSSL(sslOptions);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SSL support is required but is not available in the JVM.", e);
            }
        }

        Cluster cluster = clusterBuilder.build();
        cluster.init();

        Session session = cluster.connect();

        return session;
    }

    /**
     * Creates a keyspace if it does not already exist
     * @param keyspace The name of the keyspace
     */
    private void ensureKeyspace(String keyspace) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;");
    }

    private boolean hasMappingTable() {
        if (session.getCluster().getMetadata().getKeyspace("openshift_metrics").getTable("metrics_mappings") == null) {
            log.debug("metrics_mappings table does not exist");
            return false;
        } else {
            log.debug("metrics_mapping table does exist");
        }

        ResultSet resultset = session.execute("SELECT project_name FROM openshift_metrics.metrics_mappings WHERE project_id='%succeeded'");

        Row row = (resultset == null) ? null: resultset.one();
        String succeeded = (row == null) ? "false" : row.getString("project_name");

        if (succeeded.equalsIgnoreCase("true")) {
            log.debug("metrics_mapping table succeeded when it was created.");
            return true;
        } else {
            log.debug("metrics_mapping table was created, but it did not succeed the last time.");
            return false;
        }
    }

    private void createMappingTable() {
        session.execute("CREATE TABLE IF NOT EXISTS openshift_metrics.metrics_mappings (project_id text PRIMARY KEY, project_name text) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };");
    }

    private Map<String,String> getMappingTable() {
        // You only want to prepare the query once. Best to do it when you initialize the session.
        PreparedStatement findMappings = session.prepare(
                "SELECT project_id, project_name " +
                        "FROM openshift_metrics.metrics_mappings " +
                        "WHERE token(project_id) > ? AND token(project_id) <= ?");

        Map<String,String> mappings = new HashMap<>();
        if (hasMappingTable()) {
            for (TokenRange tokenRange : getTokenRanges()) {
                BoundStatement boundStatement = findMappings.bind().setToken(0, tokenRange.getStart())
                        .setToken(1, tokenRange.getEnd());
                ResultSet resultSet = session.execute(boundStatement);
                resultSet.forEach(row -> mappings.put(row.getString(0), row.getString(1)));
            }
        }

        mappings.remove("%succeeded");
        return mappings;
    }

    private Set<TokenRange> getTokenRanges() {
        Set<TokenRange> tokenRanges = new HashSet<>();
        for (TokenRange tokenRange : session.getCluster().getMetadata().getTokenRanges()) {
            tokenRanges.addAll(tokenRange.unwrap());
        }
        return tokenRanges;
    }

    private void writeMappingTable(Map<String,String> mappings) {
        createMappingTable();

        PreparedStatement pStatement = session.prepare("INSERT INTO openshift_metrics.metrics_mappings (project_id, project_name) VALUES(?,?);");

        for (Map.Entry<String,String> entry: mappings.entrySet()) {
            int maxRetries = 3;
            int tries = 0;
            boolean succeeded = false;
            while (!succeeded && tries < maxRetries) {
                try {
                    log.debugf("trying to write value to metrics_mapping table: %s %s", entry.getKey(), entry.getValue());
                    session.execute(pStatement.bind(entry.getKey(), entry.getValue()));
                    log.debugf("wrote value to metrics_mapping table");
                    succeeded = true;
                } catch (Exception e) {
                    log.errorf(e, "Error trying to insert entry into the openshift_metrics.metrics_mappings table (%s:%s). Will retry.", entry.getKey(), entry.getValue());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        break;
                    }
                }
                tries++;
            }

            if (!succeeded) {
                throw new RuntimeException("Could not write all values to the openshift_metrics.metrics_mapping table. Aborting");
            }
        }

        session.execute(pStatement.bind("%succeeded", "true"));
    }

    private Set<String> getTenants() {
        Set<String> tenants = new HashSet<>();
        ResultSet resultset = session.execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'hawkular_metrics';");
        if (!resultset.iterator().hasNext()) {
            return tenants;
        }

        try {
            // An invalid query exception will occur if the table does not exists.
            // If the table does not exist, then no tenants have been stored yet so we just return the empty tenant set.
            ResultSet resultSet = session.execute("SELECT DISTINCT tenant_id,type from hawkular_metrics.metrics_idx;");

            Iterator<Row> ri = resultSet.iterator();
            while (ri.hasNext()) {
                Row row = ri.next();
                String tenant = row.getString("tenant_id");
                if (!tenant.startsWith("_") && !tenant.contains(":")) {
                    tenants.add(tenant);
                }
            }
        } catch (InvalidQueryException iqe) {
            log.warn(iqe);
        }
        return tenants;
    }

    private Map<String,String> getNamespaceMapping(Set<String> tenants) {
        Map<String,String> namespaceMap = new HashMap<>();

        if (tenants == null || tenants.isEmpty()) {
            return namespaceMap;
        }

        try {
            String path = "api/v1/namespaces";
            URL url = new URL(NamespaceHandler.KUBERNETES_MASTER_URL + "/" + path);

            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization", "Bearer " + token);

            // set to 0 which indicated an infinite timeout
            connection.setConnectTimeout(0);
            connection.setReadTimeout(0);

            connection.connect();
            int responseCode = connection.getResponseCode();

            if (responseCode == 200) {

                JsonFactory jsonFactory = new JsonFactory();
                jsonFactory.setCodec(new ObjectMapper());

                InputStream inputStream = connection.getInputStream();
                JsonParser jsonParser = jsonFactory.createParser(inputStream);

                JsonNode jsonNode = jsonParser.readValueAsTree();
                // When the connection is closed, the response is null
                if (jsonNode != null) {
                    JsonNode items = jsonNode.get("items");
                    if (items != null && items.isArray()) {
                        for (JsonNode project : items) {
                            JsonNode metaData = project.get("metadata");
                            String projectName = metaData.get("name").asText();
                            String projectID = metaData.get("uid").asText();

                            if (tenants.contains(projectName)) {
                                namespaceMap.put(projectID, projectName);
                            }
                        }
                    }
                }

                jsonParser.close();
                inputStream.close();

            } else {
                log.error("Error getting metadata from the OpenShift Master (" + responseCode + "). This may mean the 'hawkular' user does not have permission to watch projects. Waiting 2 seconds and will try again.");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }


        } catch (IOException e) {
            log.error(e);
        }

        return namespaceMap;
    }
}
