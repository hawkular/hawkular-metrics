/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.jobs;

/**
 * @author Michael Burman
 */
public interface JobsServiceImplMBean {

    /**
     * Execute compression job that includes the given timestamp (automatic range calculation)
     *
     * @param timestamp A timestamp inside the block (inclusive)
     */
    void submitCompressJob(long timestamp);

    /**
     * Execute compression job that includes the given timestamp (automatic range calculation) with
     * configurable blockSize.
     *
     * @param timestamp A timestamp inside the block (inclusive)
     * @param blockSize blockSize in ISO8601 format @see <a href="https://docs.oracle
     *                  .com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence">Duration</a>
     */
    void submitCompressJob(long timestamp, String blockSize);

}
