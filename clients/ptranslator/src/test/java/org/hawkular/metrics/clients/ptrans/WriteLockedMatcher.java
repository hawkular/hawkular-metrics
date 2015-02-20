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
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * @author Thomas Segismont
 */
public class WriteLockedMatcher extends TypeSafeMatcher<File> {
    @Override
    public void describeTo(Description description) {
        description.appendText("write locked");
    }

    @Override
    protected void describeMismatchSafely(File item, Description mismatchDescription) {
        mismatchDescription.appendValue(item).appendText(" is not write locked");
    }

    @Override
    protected boolean matchesSafely(File item) {
        try (
                RandomAccessFile randomAccessFile = new RandomAccessFile(item, "rw");
                FileChannel channel = randomAccessFile.getChannel();
                FileLock fileLock = channel.tryLock()
        ) {
            return fileLock == null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Factory
    public static Matcher<File> writeLocked() {
        return new WriteLockedMatcher();
    }
}
