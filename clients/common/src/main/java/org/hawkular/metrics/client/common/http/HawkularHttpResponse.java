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
package org.hawkular.metrics.client.common.http;

/**
 * Minimalist and API-agnostic response wrapper
 * @author Joel Takvorian
 */
public class HawkularHttpResponse {

    private final String content;
    private final int responseCode;
    private final String errorMsg;

    public HawkularHttpResponse(String content, int responseCode) {
        this.content = content;
        this.responseCode = responseCode;
        this.errorMsg = null;
    }

    public HawkularHttpResponse(String content, int responseCode, String errorMsg) {
        this.content = content;
        this.responseCode = responseCode;
        this.errorMsg = errorMsg;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public String getContent() {
        return content;
    }
}
