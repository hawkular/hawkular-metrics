/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.toList;

import static org.hawkular.metrics.clients.ptrans.util.ProcessUtil.kill;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.hawkular.metrics.clients.ptrans.ConfigurationKey;
import org.hawkular.metrics.clients.ptrans.Service;
import org.hawkular.metrics.clients.ptrans.data.Point;
import org.hawkular.metrics.clients.ptrans.util.PrintOutputOnFailureWatcher;
import org.junit.After;
import org.junit.Rule;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

import jnr.constants.platform.Signal;

/**
 * @author Thomas Segismont
 */
public class CollectdITest extends FullStackITest {
    private static final String COLLECTD_PATH = System.getProperty("collectd.path", "/usr/sbin/collectd");

    private File collectdConfFile;
    private File collectdOut;
    private File collectdErr;
    private ProcessBuilder collectdProcessBuilder;
    private Process collectdProcess;

    @Rule
    public final PrintOutputOnFailureWatcher collectdOutputRule = new PrintOutputOnFailureWatcher(
            "collectd",
            () -> collectdOut,
            () -> collectdErr
    );

    @Override
    protected void configureSource() throws Exception {
        assumeCollectdIsPresent();
        collectdConfFile = temporaryFolder.newFile();
        try (FileOutputStream out = new FileOutputStream(collectdConfFile)) {
            Resources.copy(Resources.getResource("collectd.conf"), out);
        }
        assertCollectdConfIsValid();
    }

    private void assumeCollectdIsPresent() {
        Path path = Paths.get(COLLECTD_PATH);
        assumeTrue(COLLECTD_PATH + " does not exist", Files.exists(path));
        assumeTrue(COLLECTD_PATH + " is not a file", Files.isRegularFile(path));
        assumeTrue(COLLECTD_PATH + " is not executable", Files.isExecutable(path));
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

    @Override
    protected void changePTransConfig(Properties properties) {
        properties.setProperty(ConfigurationKey.SERVICES.toString(), Service.COLLECTD.getExternalForm());
        properties.setProperty(ConfigurationKey.SERVICES_COLLECTD_PORT.toString(), String.valueOf(35826));
    }

    @Override
    protected void startSource() throws Exception {
        File stdbuf = new File("/usr/bin/stdbuf");
        ImmutableList.Builder<String> collectdCmd = ImmutableList.builder();
        if (stdbuf.exists() && stdbuf.canExecute()) {
            collectdCmd.add(stdbuf.getAbsolutePath(), "-o0", "-e0");
        }
        collectdCmd.add(COLLECTD_PATH, "-C", collectdConfFile.getAbsolutePath(), "-f");
        collectdProcessBuilder.command(collectdCmd.build());
        collectdProcess = collectdProcessBuilder.start();
    }

    @Override
    protected void waitForSourceValues() throws Exception {
        long c;
        do {
            Thread.sleep(MILLISECONDS.convert(1, SECONDS));
            c = Files.lines(collectdOut.toPath())
                     .filter(l -> l.startsWith("PUTVAL"))
                     .collect(counting());
        } while (c < 1);
    }

    @Override
    protected void stopSource() throws Exception {
        kill(collectdProcess, Signal.SIGUSR1); // Flush data
        kill(collectdProcess, Signal.SIGTERM);
        collectdProcess.waitFor();
    }

    @Override
    protected List<Point> getExpectedData() throws Exception {
        return Files.lines(collectdOut.toPath())
                    .filter(l -> l.startsWith("PUTVAL"))
                    .map(this::collectdLogToPoint)
                    .collect(toList());
    }

    private Point collectdLogToPoint(String line) {
        String[] split = line.split(" ");
        assertEquals("Unexpected format: " + line, 4, split.length);
        String metric = split[1].replace('/', '.').replace('-', '.');
        String[] data = split[3].split(":");
        assertEquals("Unexpected format: " + line, 2, data.length);
        long timestamp = Long.parseLong(data[0].replace(".", ""));
        if (!data[0].contains(".")) {
            // collectd v4 does not have high resolution timestamps
            timestamp *= 1000;
        }
        double value = Double.parseDouble(data[1]);
        return new Point(metric, timestamp, value);
    }

    @Override
    protected void checkTimestamps(String failureMsg, long expected, long actual) {
        assertTrue(failureMsg, Math.abs(expected - actual) < 2);
    }

    @After
    public void tearDown() throws Exception {
        if (collectdProcess != null && collectdProcess.isAlive()) {
            collectdProcess.destroy();
        }
    }
}
