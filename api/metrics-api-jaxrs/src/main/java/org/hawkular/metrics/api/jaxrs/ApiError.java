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
package org.hawkular.metrics.api.jaxrs;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * Return information what failed in the REST-call.
 * @author Michael Burman
 */
@ApiModel(description = "If REST-call returns other than success, detailed error is returned.")
public class ApiError {
    private String errorMsg;

    public ApiError(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    @ApiModelProperty(value = "Detailed error message of what happened")
    public String getErrorMsg() {
        return errorMsg;
    }
}
