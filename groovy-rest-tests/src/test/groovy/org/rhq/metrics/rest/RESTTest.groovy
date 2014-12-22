package org.rhq.metrics.rest

import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient
import org.junit.BeforeClass

class RESTTest {

  static baseURI = System.getProperty('rhq-metrics.base-uri') ?: '127.0.0.1:8080/rhq-metrics'
  static RESTClient rhqm

  @BeforeClass
  static void initClient() {
    rhqm = new RESTClient("http://$baseURI/", ContentType.JSON)
  }

}
