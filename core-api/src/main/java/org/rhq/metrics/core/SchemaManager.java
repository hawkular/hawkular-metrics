package org.rhq.metrics.core;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * @author John Sanda
 * @author Heiko W. Rupp
 */
public class SchemaManager {

    private Session session;

    public SchemaManager(Session session) {
        this.session = session;
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

