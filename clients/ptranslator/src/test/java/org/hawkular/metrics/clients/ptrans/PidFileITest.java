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
package org.hawkular.metrics.clients.ptrans;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.assertj.core.api.Condition;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @author Thomas Segismont
 */
public class PidFileITest extends ExecutableITestBase {

    @Test
    public void shouldWritePidToPidFile() throws Exception {
        ptransProcessBuilder.command().addAll(
                Lists.newArrayList(
                        "-c", ptransConfFile.getAbsolutePath(),
                        "-p", ptransPidFile.getAbsolutePath()
                )
        );

        ptransProcess = ptransProcessBuilder.start();
        boolean isRunning = ptransProcess.isAlive();
        for (int i = 0; !isRunning && i < 5; i++) {
            isRunning = !ptransProcess.waitFor(1, SECONDS);
        }
        assertThat(isRunning).isTrue();

        boolean pidFileWritten = ptransPidFile.canRead() && ptransPidFile.length() > 0;
        for (int i = 0; !pidFileWritten && i < 5; i++) {
            Thread.sleep(MILLISECONDS.convert(1, SECONDS));
            pidFileWritten = ptransPidFile.canRead() && ptransPidFile.length() > 0;
        }
        assertThat(ptransPidFile).isFile().canRead().is(writeLocked());

        if (ptransProcess.getClass().getName().equals("java.lang.UNIXProcess")) {
            // On UNIX-like platforms only
            Field pidField = ptransProcess.getClass().getDeclaredField("pid");
            pidField.setAccessible(true);
            int pid = pidField.getInt(ptransProcess);
            assertThat(ptransPidFile).hasContent(String.valueOf(pid));
        }
    }

    @Test
    public void shouldExitWithErrorIfPidFileIsLocked() throws Exception {
        shouldWritePidToPidFile();

        ptransProcessBuilder.redirectOutput(temporaryFolder.newFile());
        File ptransErrBis = temporaryFolder.newFile();
        ptransProcessBuilder.redirectError(ptransErrBis);
        Process ptransProcessBis = ptransProcessBuilder.start();
        int returnCode = ptransProcessBis.waitFor();
        assertThat(returnCode).isNotEqualTo(0);

        String expectedMessage = String.format(
                "Unable to lock PID file %s, another instance is probably running.",
                ptransPidFile.getAbsolutePath()
        );
        assertThat(ptransErrBis).isFile().canRead().hasContent(expectedMessage);
    }

    private Condition<? super File> writeLocked() {
        return new Condition<File>() {
            @Override
            public boolean matches(File file) {
                try (
                        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                        FileChannel channel = randomAccessFile.getChannel();
                        FileLock fileLock = channel.tryLock()
                ) {
                    return fileLock == null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
