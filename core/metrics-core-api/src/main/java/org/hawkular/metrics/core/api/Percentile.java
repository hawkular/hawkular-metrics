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
package org.hawkular.metrics.core.api;

/**
 * @author Michael Burman
 */
public class Percentile {
    private double target;
    private double value = 0.0;

    public Percentile(double quantile) {
        this.target = quantile;
    }

    public Percentile(double quantile, double value) {
        this.target = quantile;
        this.value = value;
    }

    public double getQuantile() {
        return target;
    }

    public void setQuantile(double target) {
        this.target = target;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
