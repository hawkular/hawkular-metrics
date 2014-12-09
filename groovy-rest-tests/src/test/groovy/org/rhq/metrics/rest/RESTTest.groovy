package org.rhq.metrics.rest

import groovyx.net.http.RESTClient
import org.junit.BeforeClass

class RESTTest {

  static RESTClient rhqm

  @BeforeClass
  static void initClient() {
    String baseURI = System.getProperty('rhq-metrics.base-uri') ?: '127.0.0.1:8080/rhq-metrics'
    rhqm = new RESTClient("http://$baseURI/", groovyx.net.http.ContentType.JSON)
  }

}
