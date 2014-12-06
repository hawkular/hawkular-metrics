import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import org.rhq.metrics.impl.cassandra.MetricsServiceCassandra
import org.testng.annotations.BeforeSuite
import org.testng.annotations.Test
/**
 * Created by IntelliJ IDEA.
 * User: John Sanda
 * Date: 12/4/14
 * Time: 4:47 PM
 * To change this template use File | Settings | File Templates.
 */
class FakeITest {

  Session session;
  MetricsServiceCassandra metricsServer;
  String keyspace = System.getProperty("keyspace") ?: "rhq_metrics_rest_tests"

  @BeforeSuite
  void initSuite() {
    Cluster cluster = new Cluster.Builder().addContactPoints("127.0.0.1").build()
    session = cluster.connect(keyspace)
  }

  @Test
  void run() {
//    Table.each { session.execute("TRUNCATE $it") }
//
//    def rhqm = new RESTClient("http://127.0.0.1:8080/rhq-metrics/", JSON)
//
//    def response = rhqm.post(path: 'tenants', body: [id: 'tenant-1'])
//    assertEquals(response.status, 200)
//
//    response = rhqm.get(path: "tenants")
//    assertEquals(response.status, 200)
//
//    def expectedData = [
//      [id: MetricsService.DEFAULT_TENANT_ID, retentions: [:]],
//      [id: 'tenant-1', retentions: [:]]
//    ]
//    assertEquals(response.data, expectedData, 'Response data does not match')
  }

}
