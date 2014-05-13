import groovy.text.SimpleTemplateEngine

import javax.management.MBeanServerConnection
import javax.management.ObjectName
import javax.management.remote.JMXConnector
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL

version = "2.0.7"
nodeDir = new File(clusterDir(), "node0")

if (nodeDir.exists()) {
  log.info "Cluster is already installed in ${clusterDir()}"
} else {
  log.info "Installing cluster in ${clusterDir()}"

  downloadCassandra()
  File repositoryDir = new File(clusterDir(), "repository")
  repositoryDir.mkdirs()
  def cassandraDir = new File(repositoryDir, "apache-cassandra-$version")
  if (!cassandraDir.exists()) {
    ant.untar(
            src: "${settings.localRepository}/org/apache/cassandra/apache-cassandra/${version}/apache-cassandra-2.0.7-bin.tar.gz",
            compression: "gzip",
            dest: repositoryDir
    )
  }
  ant.copy(todir: nodeDir) {
    fileset(dir: "${repositoryDir.absolutePath}/apache-cassandra-2.0.7", includes: "**/*")
  }
  updateConfig()
  updatePerms()
  startCassandra()
}

File clusterDir() {
  String userHomeDir = new File(System.getProperty("user.home"))
  File rhqMetricsDir = new File(userHomeDir, ".rhq-metrics")

  return rhqMetricsDir
}

def downloadCassandra() {
  def version = "2.0.7"
  def downloadDir = new File("${settings.localRepository}/org/apache/cassandra/apache-cassandra/${version}")
  downloadDir.mkdirs()
  ant.get(
      src: "http://central.maven.org/maven2/org/apache/cassandra/apache-cassandra/2.0.7/apache-cassandra-2.0.7-bin.tar.gz",
      dest: downloadDir,
      skipexisting: true
  )
}

def updateConfig() {
  def confDir = new File(nodeDir, "conf")
  def yamlFileTemplate = new File(basedir, "src/main/script/cassandra.yaml")
  def model = [
      clusterName: "rhq-metrics",
      dataFileDir: new File(nodeDir, "data").absolutePath,
      commitLogDir: new File(nodeDir, "commit_log").absolutePath,
      savedCachesDir: new File(nodeDir, "saved_caches").absolutePath
  ]
  def updatedYamlFile = new File(confDir, "cassandra.yaml")
  new SimpleTemplateEngine().createTemplate(yamlFileTemplate).make(model).writeTo(new FileWriter(updatedYamlFile))

  def log4jTemplate = new File(basedir, "src/main/script/log4j-server.properties")
  model = [logFile: new File(nodeDir, "logs").absolutePath]
  def updateLog4jFile = new File(confDir, "log4j-server.properties")
  new SimpleTemplateEngine().createTemplate(log4jTemplate).make(model).writeTo(new FileWriter(updateLog4jFile))
}

def updatePerms() {
  def binDir = new File(nodeDir, "bin")
  def confDir = new File(nodeDir, "conf")
  ant.chmod(dir: binDir, excludes: "*.bat", perm: "+x")
}

def startCassandra() {
  def binDir = new File(nodeDir, "bin")
  ProcessBuilder processBuilder = new ProcessBuilder("./cassandra", "-p", "cassandra.pid")
  processBuilder.directory(binDir)//.redirectErrorStream(true).redirectOutput()
  processBuilder.directory(binDir).inheritIO()
  def process = processBuilder.start()
  waitForNodeToInitialize()
}

def waitForNodeToInitialize() {
  String url = "service:jmx:rmi:///jndi/rmi://127.0.0.1:7199/jmxrmi"
  JMXServiceURL serviceURL = new JMXServiceURL(url);
  JMXConnector connector = null;
  MBeanServerConnection serverConnection = null;

  log.info "Waiting for cluster to initialize..."
  def timeout = 2000
  Thread.sleep(timeout)
  def retries = 4

  for (int i = 0; i < retries; ++i) {
    try {
      connector = JMXConnectorFactory.connect(serviceURL, new HashMap());
      serverConnection = connector.getMBeanServerConnection();
      ObjectName storageService = new ObjectName("org.apache.cassandra.db:type=StorageService");
      def nativeTransportRunning = serverConnection.getAttribute(storageService, "NativeTransportRunning");
      if (nativeTransportRunning) {
        return
      }
    } catch (Exception e) {
      log.info("Node is not initialized yet", e)
      Thread.sleep(timeout * (i + 1));
    }
  }
  fail("Unable to verify that the cluster is initialized")
}