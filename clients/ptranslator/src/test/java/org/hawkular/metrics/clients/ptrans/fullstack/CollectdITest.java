/*
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
package org.hawkular.metrics.clients.ptrans.fullstack;

import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.BATCH_DELAY;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.BATCH_SIZE;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

import org.hawkular.metrics.clients.ptrans.ExecutableITestBase;
import org.hawkular.metrics.clients.ptrans.PrintOutputOnFailureWatcher;
import org.hawkular.metrics.clients.ptrans.Service;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

/**
 * @author Thomas Segismont
 */
public class CollectdITest extends ExecutableITestBase {
    private static final String COLLECTD_PATH = System.getProperty("collectd.path", "/usr/sbin/collectd");

    private File collectdConfFile;
    private File collectdOut;
    private File collectdErr;
    private ProcessBuilder collectdProcessBuilder;
    private Process collectdProcess;

    @Rule
    public final PrintOutputOnFailureWatcher collectdOutputRule = new PrintOutputOnFailureWatcher(
            "colletcd",
            () -> collectdOut,
            () -> collectdErr
    );

    @Before
    public void setUp() throws Exception {
        assumeCollectdIsPresent();
        configureCollectd();
        assertCollectdConfIsValid();
        configurePTrans();
    }

    private void assumeCollectdIsPresent() {
        Path path = Paths.get(COLLECTD_PATH);
        assumeTrue(COLLECTD_PATH + " does not exist", Files.exists(path));
        assumeTrue(COLLECTD_PATH + " is not a file", Files.isRegularFile(path));
        assumeTrue(COLLECTD_PATH + " is not executable", Files.isExecutable(path));
    }

    private void configureCollectd() throws Exception {
        collectdConfFile = temporaryFolder.newFile();
        try (FileOutputStream out = new FileOutputStream(collectdConfFile)) {
            Resources.copy(Resources.getResource("collectd.conf"), out);
        }
    }

    private void assertCollectdConfIsValid() throws Exception {
        collectdOut = temporaryFolder.newFile();
        collectdErr = temporaryFolder.newFile();

        collectdProcessBuilder = new ProcessBuilder();
        collectdProcessBuilder.directory(temporaryFolder.getRoot());
        collectdProcessBuilder.redirectOutput(collectdOut);
        collectdProcessBuilder.redirectError(collectdErr);

        collectdProcessBuilder.command(COLLECTD_PATH, "-C", collectdConfFile.getAbsolutePath(), "-t", "-T", "-f");

        collectdProcess = collectdProcessBuilder.start();

        int exitCode = collectdProcess.waitFor();
        assertEquals("Collectd configuration doesn't seem to be valid", 0, exitCode);

        boolean hasErrorInLog = Stream.concat(Files.lines(collectdOut.toPath()), Files.lines(collectdErr.toPath()))
                                      .anyMatch(l -> l.contains("[error]"));
        assertFalse("Collectd configuration doesn't seem to be valid", hasErrorInLog);
    }

    public void configurePTrans() throws Exception {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(ptransConfFile)) {
            properties.load(in);
        }
        properties.setProperty(SERVICES.getExternalForm(), Service.COLLECTD.getExternalForm());
        properties.setProperty(BATCH_DELAY.getExternalForm(), String.valueOf(1));
        properties.setProperty(BATCH_SIZE.getExternalForm(), String.valueOf(1));
        try (OutputStream out = new FileOutputStream(ptransConfFile)) {
            properties.store(out, "");
        }
    }

    @Test
    public void shouldFindCollectdMetricsOnServer() throws Exception {
        ptransProcessBuilder.command().addAll(ImmutableList.of("-c", ptransConfFile.getAbsolutePath()));
        ptransProcess = ptransProcessBuilder.start();
        assertPtransHasStarted(ptransProcess, ptransOut);

        collectdProcessBuilder.command(COLLECTD_PATH, "-C", collectdConfFile.getAbsolutePath(), "-f");
        collectdProcess = collectdProcessBuilder.start();

        fail("Not implemented yet");
    }

    @After
    public void tearDown() {
        if (collectdProcess != null && collectdProcess.isAlive()) {
            collectdProcess.destroy();
        }
    }
}
