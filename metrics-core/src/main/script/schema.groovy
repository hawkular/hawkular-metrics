import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import org.rhq.metrics.core.SchemaManager

Cluster cluster = new Cluster.Builder()
        .addContactPoint("127.0.0.1")
        .withoutJMXReporting()
        .build()
Session session = cluster.connect()

String keyspace = properties["keyspace"] ?: "rhqtest"
SchemaManager schemaManager = new SchemaManager(session)
schemaManager.createSchema()
