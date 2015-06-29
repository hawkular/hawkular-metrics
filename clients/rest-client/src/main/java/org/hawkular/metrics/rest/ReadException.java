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
package org.hawkular.metrics.rest;

/**
 * Thrown if a read request fails on the server. The server will send back a status code other than 200 when this
 * happens. The server returns a 204 when there is no data, but this exception is not thrown in that case.
 */
public class ReadException extends RuntimeException {

    private int status;

    public ReadException(String message, int status) {
        super(message);
        this.status = status;
    }

    /**
     * The HTTP status code reported by the server
     */
    public int getStatus() {
        return status;
    }
}
