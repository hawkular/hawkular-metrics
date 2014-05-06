package org.rhq.metrics.core;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * @author John Sanda
 */
public class SchemaManager {

    private static final String KEYSPACE = "rhq";

    private Session session;

    public SchemaManager(Session session) {
        this.session = session;
    }

    public void updateSchema() {
        ResultSet resultSet = session.execute(
            "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '" + KEYSPACE + "'");

        if (!resultSet.isExhausted()) {
            return;
        }

        session.execute(
            "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        session.execute(
            "CREATE TABLE rhq.metrics ( " +
                "bucket varchar, " +
                "metric_id varchar, " +
                "time timestamp, " +
                "value map<int, double>, " +
                "PRIMARY KEY (bucket, metric_id, time) " +
            ")"
        );
    }

}
