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
package org.hawkular.metrics.clients.ptrans.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.Locale;
import java.util.function.Supplier;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * A JUnit test watcher which prints an external process output when the test fails.
 *
 * @author Thomas Segismont
 */
public class PrintOutputOnFailureWatcher extends TestWatcher {
    private static final String SEPARATOR = "###########";

    private final String processName;
    private final Supplier<File> ptransOut;
    private final Supplier<File> ptransErr;

    /**
     * @param processName external process name
     * @param outSupplier external process output supplier
     * @param errSupplier external process error supplier
     */
    public PrintOutputOnFailureWatcher(String processName, Supplier<File> outSupplier, Supplier<File> errSupplier) {
        this.processName = processName;
        this.ptransOut = outSupplier;
        this.ptransErr = errSupplier;
    }

    @Override
    protected void failed(Throwable e, Description description) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        printWriter.printf(Locale.ROOT, "%s, %s output%n", description.getMethodName(), processName);
        printWriter.println(SEPARATOR);
        try {
            Files.readAllLines(ptransOut.get().toPath()).forEach(printWriter::println);
        } catch (IOException ignored) {
        }
        printWriter.println(SEPARATOR);
        printWriter.printf(Locale.ROOT, "%s, %s err%n", description.getMethodName(), processName);
        printWriter.println(SEPARATOR);
        try {
            Files.readAllLines(ptransErr.get().toPath()).forEach(printWriter::println);
        } catch (IOException ignored) {
        }
        printWriter.println(SEPARATOR);
        printWriter.close();
        System.out.println(stringWriter.toString());
    }
}
