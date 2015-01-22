/**
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
