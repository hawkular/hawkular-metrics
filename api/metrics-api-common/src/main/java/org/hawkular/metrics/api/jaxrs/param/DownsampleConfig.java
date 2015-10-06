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

import org.hawkular.metrics.core.api.Downsample.Method;
import org.hawkular.metrics.core.api.Downsample.Operation;

/**
 * A JAX-RS parameter object used to build downsampling configuration from query params.
 *
 * @author Stefan Negrea
 */
public class DownsampleConfig {

    private Method method;
    private Operation operation;
    private String problem;
    private boolean valid;

    public DownsampleConfig(String method, String operation) {
        if (method == null) {
            this.method = Method.Group;
            this.operation = null;
            this.valid = true;
            this.problem = null;
        } else {
            try {
                this.method = Method.valueOf(method);
            } catch (IllegalArgumentException e) {
                this.method = null;
                this.operation = null;
                this.valid = false;
                this.problem = "Invalid downsampling method. Only Group and Individual are supported.";
                return;
            }

            if (Method.Group.equals(this.method)) {
                this.operation = null;
                this.valid = true;
                this.problem = null;
            } else {
                if (operation == null) {
                    this.operation = Operation.Sum;
                } else {
                    try {
                        this.operation = Operation.valueOf(operation);
                        this.valid = true;
                        this.problem = null;
                    } catch (IllegalArgumentException e) {
                        this.method = null;
                        this.operation = null;
                        this.valid = false;
                        this.problem = "Invalid downsampling operation. Only Average and Sum are supported.";
                    }
                }
            }
        }
    }

    public Method getMethod() {
        return method;
    }

    public Operation getOperation() {
        return operation;
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
                .add("method", method.toString())
                .add("operation", operation.toString())
                .add("valid", valid)
                .add("problem", problem)
                .omitNullValues()
                .toString();
    }
}
