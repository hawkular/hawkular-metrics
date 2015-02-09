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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;

/**
 * Helper class to manage a PID file.
 *
 * @author Thomas Segismont
 */
class PidFile {

    private final File file;
    private FileChannel channel;
    private FileLock fileLock;

    /**
     * @param file PID file reference
     */
    PidFile(File file) {
        this.file = file;
    }

    /**
     * Try to acquire a write lock on the PID file and, on success, write the {@code pid} to it and mark it for deletion
     * on exit.
     * <p>
     * On failure to open the file, acquire the lock or write to the file, an error message will be printed to {@link
     * java.lang.System#err}.
     * <p>
     * The caller must call {@link #release()} before the JVM stops, regardless of the the result of this method.
     *
     * @param pid the process id to write
     *
     * @return true if the lock was acquired and the {@code pid} written to the PID file, false otherwise
     */
    boolean tryLock(int pid) {
        try {
            channel = new RandomAccessFile(file, "rw").getChannel();
        } catch (FileNotFoundException e) {
            System.err.printf("Unable to open PID file %s for writing.%n", file.getAbsolutePath());
            return false;
        }
        try {
            fileLock = channel.tryLock();
        } catch (IOException e) {
            System.err.printf("Unable to lock PID file %s.%n", file.getAbsolutePath());
            return false;
        }
        if (fileLock == null) {
            System.err.printf("Unable to lock PID file %s, another instance is probably running.%n",
                file.getAbsolutePath());
            return false;
        }
        try {
            channel.truncate(0);
        } catch (IOException e) {
            System.err.printf("Unable to truncate PID file %s.%n", file.getAbsolutePath());
            return false;
        }
        byte[] bytes = String.format("%d%n", pid).getBytes(StandardCharsets.US_ASCII);
        try {
            channel.write(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            System.err.printf("Unable to write to PID file %s.%n", file.getAbsolutePath());
            return false;
        }
        file.deleteOnExit();
        return true;
    }

    /**
     * Release resources used to managed the PID file.
     */
    void release() {
        if (fileLock != null) {
            try {
                fileLock.release();
            } catch (IOException ignored) {
            }
        }
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }
    }

}
