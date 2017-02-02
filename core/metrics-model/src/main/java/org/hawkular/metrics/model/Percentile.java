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
package org.hawkular.metrics.model;

/**
 * @author Michael Burman
 */
public class Percentile {

    private double target;
    private double value = 0.0;
    private String originalQuantile;

    public Percentile(String quantile) {
        this.originalQuantile = quantile;
        this.target = Double.valueOf(quantile);
    }

    public Percentile(String quantile, double value) {
        this.originalQuantile = quantile;
        this.value = value;
        this.target = Double.valueOf(quantile);
    }

    public String getOriginalQuantile() {
        return originalQuantile;
    }

    public double getQuantile() {
        return target;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "p(" + originalQuantile + ")=" + value;
    }
}
