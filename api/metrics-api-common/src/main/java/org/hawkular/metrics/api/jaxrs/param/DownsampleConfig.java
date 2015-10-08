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

package org.hawkular.metrics.api.jaxrs.param;

/**
 * A JAX-RS parameter object used to build downsampling configuration from query params.
 *
 * @author Stefan Negrea
 */
public class DownsampleConfig {

    public enum Method {
        Simple, Sum
    }

    private Method downsampleMethod;
    private String problem;
    private boolean valid;

    public DownsampleConfig(String method) {
        if (method == null) {
            this.downsampleMethod = Method.Simple;
            this.valid = true;
            this.problem = null;
        } else {
            try {
                this.downsampleMethod = Method.valueOf(method);
                this.valid = true;
                this.problem = null;
            } catch (IllegalArgumentException e) {
                this.downsampleMethod = null;
                this.valid = false;
                this.problem = "Invalid downsampling method. Only Simple and Sum are supported.";
                return;
            }
        }
    }

    public Method getDownsampleMethod() {
        return downsampleMethod;
    }

    public boolean isValid() {
        return valid;
    }

    public String getProblem() {
        return problem;
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("downsampleMethod", downsampleMethod.toString())
                .add("valid", valid)
                .add("problem", problem)
                .omitNullValues()
                .toString();
    }
}
