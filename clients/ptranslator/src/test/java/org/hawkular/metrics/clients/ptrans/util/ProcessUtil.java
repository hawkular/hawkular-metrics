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

import java.lang.reflect.Field;

import jnr.constants.platform.Signal;
import jnr.posix.POSIXFactory;

/**
 * @author Thomas Segismont
 */
public class ProcessUtil {

    public static int getPid(Process process) throws Exception {
        testUnixProcess(process);
        Field pidField = process.getClass().getDeclaredField("pid");
        pidField.setAccessible(true);
        return pidField.getInt(process);
    }

    public static int kill(Process process, Signal signal) throws Exception {
        int pid = getPid(process);
        return POSIXFactory.getPOSIX().kill(pid, signal.intValue());
    }

    private static void testUnixProcess(Process process) {
        if (!process.getClass().getName().equals("java.lang.UNIXProcess")) {
            throw new IllegalArgumentException("Not a UNIX Process");
        }
    }
}
