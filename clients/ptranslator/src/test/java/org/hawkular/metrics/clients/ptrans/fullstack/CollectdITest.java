/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hawkular.metrics.clients.ptrans.fullstack;

import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES;
import static org.junit.Assume.assumeTrue;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.hawkular.metrics.clients.ptrans.ExecutableITestBase;
import org.hawkular.metrics.clients.ptrans.Service;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class CollectdITest extends ExecutableITestBase {
    private static final String COLLECTD_PATH = System.getProperty("collectd.path", "/usr/sbin/collectd");

    @Before
    public void collectdAssumptions() throws Exception {
        assumeCollectdIsPresent();
        configurePTrans();
    }

    @Test
    public void shouldFindCollectdMetricsOnServer() {

    }

    private void assumeCollectdIsPresent() {
        Path path = Paths.get(COLLECTD_PATH);
        assumeTrue(COLLECTD_PATH + " does not exist", Files.exists(path));
        assumeTrue(COLLECTD_PATH + " is not a file", Files.isRegularFile(path));
        assumeTrue(COLLECTD_PATH + " is not executable", Files.isExecutable(path));
    }

    public void configurePTrans() throws Exception {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(ptransConfFile)) {
            properties.load(in);
        }
        properties.setProperty(SERVICES.getExternalForm(), Service.COLLECTD.getExternalForm());
        try (OutputStream out = new FileOutputStream(ptransConfFile)) {
            properties.store(out, "");
        }
    }

}
