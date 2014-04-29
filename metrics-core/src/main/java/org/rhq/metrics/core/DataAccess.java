package org.rhq.metrics.core;

import java.util.Date;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

/**
 * @author John Sanda
 */
public class DataAccess {

    //private static final Logger logger = LoggerFactory.getLogger(DataAccess.class);

    private PreparedStatement insertData;

    private PreparedStatement findData;

    private Session session;

    public DataAccess(Session session) {
        this.session = session;
        initPreparedStatements();
    }

    private void initPreparedStatements() {
        insertData = session.prepare(
            "INSERT INTO metrics (bucket, metric_id, time, value) VALUES (?, ?, ?, ?) USING TTL ?");

        findData = session.prepare(
            "SELECT metric_id, time, value FROM metrics WHERE bucket = ? AND metric_id = ? AND time >=? AND time < ?");
    }

    public ResultSetFuture insertData(String bucket, String metricId, long timestamp, Map<Integer, Double> values,
        int ttl) {
        BoundStatement statement = insertData.bind(bucket, metricId, new Date(timestamp), values, ttl);
        return session.executeAsync(statement);
    }

    public ResultSetFuture findData(String bucket, String metricId, long startTime, long endTime) {
        BoundStatement statement = findData.bind(bucket, metricId, new Date(startTime), new Date(endTime));
        return session.executeAsync(statement);
    }

}
