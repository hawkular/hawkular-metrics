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
package org.hawkular.metrics.scheduler.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.hawkular.metrics.scheduler.api.JobParameters;

import com.google.common.collect.ImmutableMap;

import rx.Completable;

/**
 * @author jsanda
 */
public class JobParametersImpl implements JobParameters {

    private Map<String, String> parameters;

    private Function<Map<String, String>, Completable> saveParameters;

    JobParametersImpl(Map<String, String> parameters, Function<Map<String, String>, Completable> saveParameters) {
        this.parameters = new HashMap<>();
        this.parameters.putAll(parameters);
        this.saveParameters = saveParameters;
    }

    public void setSaveParameters(Function<Map<String, String>, Completable> saveParameters) {
        this.saveParameters = saveParameters;
    }

    @Override
    public String get(String key) {
        return parameters.get(key);
    }

    @Override
    public String put(String key, String value) {
        return parameters.put(key, value);
    }

    @Override
    public String remove(String key) {
        return parameters.remove(key);
    }

    @Override
    public boolean containsKey(String key) {
        return parameters.containsKey(key);
    }

    @Override
    public Map<String, String> getMap() {
        return ImmutableMap.copyOf(parameters);
    }

    @Override
    public Completable save() {
        return saveParameters.apply(parameters);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobParametersImpl that = (JobParametersImpl) o;
        return Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameters);
    }

    @Override
    public String toString() {
        return parameters.toString();
    }
}
