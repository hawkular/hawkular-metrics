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
package org.hawkular.metrics.api.util;

@SuppressWarnings("serial")
public class RequestFailure extends RuntimeException {
    private int code;

    public RequestFailure() {
      super();
      code = 500;
    }

    public RequestFailure(String message) {
      super(message);
      code = 500;
    }

    public RequestFailure(Throwable throwable) {
      super(throwable);
      code = 500;
    }

    public RequestFailure(int failureCode) {
      code = failureCode;
      initCause(new RuntimeException());
    }

    public RequestFailure(String message, Throwable throwable) {
      super(message, throwable);
      code = 500;
    }

    public RequestFailure(String message, int failureCode) {
      super(message);
      code = failureCode;
    }

    public RequestFailure(Throwable throwable, int failureCode) {
      super(throwable);
      code = failureCode;
    }

    public RequestFailure(String message, Throwable throwable, int failureCode) {
      super(message, throwable);
      code = failureCode;
    }

    public int getCode() {
        return code;
    }

    public static RequestFailure failure(Throwable throwable) {
        if (throwable instanceof RequestFailure) {
            return (RequestFailure) throwable;
        }
        if (throwable.getMessage() == null) {
            return new RequestFailure("No message provided", throwable.getCause());
        }
        return new RequestFailure(throwable.getMessage(), throwable.getCause());
    }
}