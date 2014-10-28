package org.rhq.metrics.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.io.CharStreams;

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

    public void createSchema() throws IOException {
        logger.info("Creating schema");

        ResultSet resultSet = session.execute("SELECT * FROM system.schema_keyspaces WHERE keyspace_name = 'rhq'");
        if (!resultSet.isExhausted()) {
            logger.info("Schema already exist. Skipping schema creation.");
            return;
        }

        InputStream inputStream = getClass().getResourceAsStream("/schema.cql");
        InputStreamReader reader = new InputStreamReader(inputStream);
        String content = CharStreams.toString(reader);

        for (String cql : content.split("(?m)^-- #.*$")) {
            if (!cql.startsWith("--")) {
                logger.info("Executing CQL:\n" + cql.trim() + "\n");
                session.execute(cql.trim());
            }
        }
    }

    public void updateSchema(String keyspace) {

        keyspace = keyspace.toLowerCase();

        ResultSet resultSet = session.execute(
             "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '" + keyspace + "'");


        if (resultSet.isExhausted()) {
            // No keyspace found - start from scratch
            session.execute(
                "CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        }
        // We have a keyspace - now use this for the session.
        Session session2 = session.getCluster().connect(keyspace);

        session2.execute(
            "CREATE TABLE IF NOT EXISTS metrics ( " +
                "bucket text, " +
                "metric_id text, " +
                "time timestamp, " +
                "value map<int, double>, " +
                // ( bucket, metric_id ) are a composite partition key
                "PRIMARY KEY ( (bucket, metric_id) , time) " +
            ")"
        );

        session2.execute(
            "CREATE TABLE IF NOT EXISTS counters ( " +
                "group text, " +
                "c_name text, " +
                "c_value counter, " +
                "PRIMARY KEY (group, c_name) " +
            ")"
        );
    }
}

