/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.security

import org.jboss.arquillian.junit.Arquillian
import org.junit.Test
import org.junit.runner.RunWith
/**
 * @author jsanda
 */
@RunWith(Arquillian.class)
class AuthenticationTest {

//  @Deployment
//  static WebArchive createDeployment() {
//    String projectVersion = "0.12.0-SNAPSHOT";
//    MavenResolverSystem mavenResolver = Resolvers.use(MavenResolverSystem.class);
//    List<String> deps = asList(
//        "org.hawkular.metrics:hawkular-metrics-model:" + projectVersion,
//        "org.hawkular.metrics:hawkular-metrics-api-util:" + projectVersion,
//        "org.hawkular.commons:hawkular-bus-common:0.3.2.Final",
//        "io.reactivex:rxjava:1.0.13"
//    );
//
//    Collection<JavaArchive> dependencies = new HashSet<JavaArchive>();
//    dependencies.addAll(asList(mavenResolver.loadPomFromFile("pom.xml").resolve(deps)
//        .withoutTransitivity().as(JavaArchive.class)));
//
//    WebArchive archive = ShrinkWrap.create(WebArchive.class)
//        .addPackages(true, "org.hawkular.bus", "org.hawkular.metrics.component.publish")
//        .addAsLibraries(dependencies)
//        .setManifest(new StringAsset("Manifest-Version: 1.0\nDependencies: com.google.guava\n"));
//
////        ZipExporter exporter = new ZipExporterImpl(archive);
////        exporter.exportTo(new File("target", "test-archive.war"));
//    return archive;
//  }

  @Test
  void foo() {

  }

}
