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
package org.hawkular.metrics.clients.ptrans.exec;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

import org.hawkular.metrics.clients.ptrans.util.PrintOutputOnFailureWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import com.google.common.io.Resources;

/**
 * Base class for integration tests which need to start the executable ptrans archive.
 *
 * @author Thomas Segismont
 */
public abstract class ExecutableITestBase {
    private static String JAVA;
    private static String PTRANS_ALL;

    protected TemporaryFolder temporaryFolder = new TemporaryFolder();
    protected File ptransConfFile;
    protected File ptransOut;
    protected File ptransErr;
    protected File ptransPidFile;
    protected ProcessBuilder ptransProcessBuilder;
    protected Process ptransProcess;

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(temporaryFolder)
                                                .around(
                                                        new PrintOutputOnFailureWatcher(
                                                                "ptrans",
                                                                () -> ptransOut,
                                                                () -> ptransErr
                                                        )
                                                )
                                                .around(new Timeout(1, MINUTES));

    @BeforeClass
    public static void beforeClass() {
        File javaHome = new File(System.getProperty("java.home"));
        File javaBin = new File(javaHome, "bin");
        JAVA = new File(javaBin, "java").getAbsolutePath();
        PTRANS_ALL = new File(System.getProperty("ptrans-all.path", "target/ptrans-all.jar")).getAbsolutePath();
    }

    @Before
    public void before() throws Exception {
        ptransConfFile = temporaryFolder.newFile();
        try (FileOutputStream out = new FileOutputStream(ptransConfFile)) {
            Resources.copy(Resources.getResource("ptrans.conf"), out);
        }

        ptransOut = temporaryFolder.newFile();
        ptransErr = temporaryFolder.newFile();

        ptransPidFile = temporaryFolder.newFile();

        ptransProcessBuilder = new ProcessBuilder();
        ptransProcessBuilder.directory(temporaryFolder.getRoot());
        ptransProcessBuilder.redirectOutput(ptransOut);
        ptransProcessBuilder.redirectError(ptransErr);

        ptransProcessBuilder.command(JAVA, "-Xss228k", "-jar", PTRANS_ALL);
    }

    protected void assertPtransHasStarted(Process process, File output) throws Exception {
        boolean isRunning = process.isAlive() && ptransOutputHasStartMessage(output);
        while (!isRunning) {
            isRunning = !ptransProcess.waitFor(1, SECONDS) && ptransOutputHasStartMessage(output);
        }
    }

    private boolean ptransOutputHasStartMessage(File output) throws IOException {
        return Files.lines(output.toPath()).anyMatch(line -> line.contains("ptrans started"));
    }

    @After
    public void after() {
        if (ptransProcess != null && ptransProcess.isAlive()) {
            ptransProcess.destroy();
        }
    }

}