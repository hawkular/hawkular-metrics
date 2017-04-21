/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

import java.util.Map;

import rx.Completable;

/**
 * Provides a map-like key/value API for job parameters. The parameters are mutable. Changes will be persisted after
 * job execution completes (for repeating jobs). Changes can also be persisted during execution using the
 * {@link #save() save} method.
 *
 * @author jsanda
 */
public interface JobParameters {

    /**
     * Return the value associated with the key or null if there is no such parameter
     */
    String get(String key);

    /**
     * Associates the value with the key and returns the old value if there previously was a mapping for the key
     */
    String put(String key, String value);

    /**
     * Removes the value associated with the key or null if there is no such parameter
     */
    String remove(String key);

    /**
     * Return true if the parameters contain a value for the key
     */
    boolean containsKey(String key);

    /**
     * Return an immutable map of the parameters. Note that this map is a copy. Any changes made to the parameters
     * through methods like {@link #get(String) get} or {@link #put(String, String) put} will not be reflected in
     * this map.
     */
    Map<String, String> getMap();

    /**
     * Asynchronously save the parameters back to Cassandra. For reoccurring jobs any changes to parameters will
     * automatically be persisted when the job finishes and is rescheduled for its next run. This method can be useful
     * for long running jobs that perform a lot of work. It can be used to create checkpoints so that if a job is
     * abruptly stopped and restarted it can resume its work from that checkpoint rather than starting all over.
     */
    Completable save();

}
