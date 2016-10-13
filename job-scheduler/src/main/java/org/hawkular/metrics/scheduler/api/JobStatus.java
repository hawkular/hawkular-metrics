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
package org.hawkular.metrics.scheduler.api;

/**
 * Currently only a single status is stored in the database, but it is stored as a byte in case we later decide to
 * add additional statuses.
 *
 * @author jsanda
 */
public enum JobStatus {

    /**
     * When the status column in the database is not set, the C* driver returns zero.
     */
    NONE((byte) 0),

    /**
     * Set when the job has finished execution; however, this will be done prior to other post-execution steps, namely
     * rescheduling the job (if it is repeating).
     */
    FINISHED((byte) 1);

    private byte code;

    JobStatus(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static JobStatus fromCode(byte code) {
        switch (code) {
            case 0: return NONE;
            case 1: return FINISHED;
            default: throw new IllegalArgumentException(code + " is not a recognized status code");
        }
    }
}
