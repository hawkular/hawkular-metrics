package org.rhq.metrics.core;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * @author John Sanda
 */
public class SchemaManager {

    private Session session;

    public SchemaManager(Session session) {
        this.session = session;
    }

    public void updateSchema(String keyspace) {
        ResultSet resultSet = session.execute(
             "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '" + keyspace + "'");

        if (!resultSet.isExhausted()) {
            return;
        }

        session.execute(
            "CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        session.execute(
            "CREATE TABLE " + keyspace + ".metrics ( " +
                "bucket text, " +
                "metric_id text, " +
                "time timestamp, " +
                "value map<int, double>, " +
                "PRIMARY KEY (bucket, metric_id, time) " +
            ")"
        );

        session.execute(
            "CREATE TABLE " + keyspace + ".counters ( " +
                "group text, " +
                "c_name text, " +
                "c_value counter, " +
                "PRIMARY KEY (group, c_name) " +
            ")"
        );
    }

}

