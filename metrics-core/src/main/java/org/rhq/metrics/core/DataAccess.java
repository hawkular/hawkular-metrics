package org.rhq.metrics.core;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.BatchStatement;
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

    private PreparedStatement updateCounter;

    private PreparedStatement findCountersByGroup;

    private PreparedStatement findCountersByGroupAndName;

    private PreparedStatement listNames;

    private PreparedStatement removeMetricData;

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

        updateCounter = session.prepare(
            "UPDATE counters " +
            "SET c_value = c_value + ? " +
            "WHERE group = ? AND c_name = ?");

        findCountersByGroup = session.prepare("SELECT group, c_name, c_value FROM counters WHERE group = ?");

        findCountersByGroupAndName = session.prepare(
            "SELECT group, c_name, c_value FROM counters WHERE group = ? AND c_name IN ?");

        listNames = session.prepare("SELECT DISTINCT bucket, metric_id FROM metrics ");

        removeMetricData = session.prepare("DELETE FROM metrics WHERE bucket = 'raw' AND metric_id = ?"); // TODO all buckets
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

    public ResultSetFuture updateCounter(Counter counter) {
        BoundStatement statement = updateCounter.bind(counter.getValue(), counter.getGroup(), counter.getName());
        return session.executeAsync(statement);
    }

    public ResultSetFuture updateCounters(Collection<Counter> counters) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.COUNTER);
        for (Counter counter : counters) {
            batchStatement.add(updateCounter.bind(counter.getValue(), counter.getGroup(), counter.getName()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture findCounters(String group) {
        BoundStatement statement = findCountersByGroup.bind(group);
        return session.executeAsync(statement);
    }

    public ResultSetFuture findCounters(String group, List<String> names) {
        BoundStatement statement = findCountersByGroupAndName.bind(group, names);
        return session.executeAsync(statement);
    }

    public ResultSetFuture listMetricNames() {
        BoundStatement statement = listNames.bind();
        return session.executeAsync(statement);
    }

    public ResultSetFuture removeData(String id) {
        BoundStatement statement = removeMetricData.bind(id);
        return session.executeAsync(statement);
    }

}
