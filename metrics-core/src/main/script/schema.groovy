import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session

Cluster cluster = new Cluster.Builder()
        .addContactPoint("127.0.0.1")
        .withoutJMXReporting()
        .build()
Session session = cluster.connect()
create(session)

def create(Session session) {
  session.execute(
    "CREATE KEYSPACE rhq WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
  )

  session.execute("""
    CREATE TABLE rhq.metrics (
        bucket varchar,
        metric_id varchar,
        time timestamp,
        value map<int, double>,
        PRIMARY KEY (bucket, metric_id, time)
      ) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };
"""
  )
}
