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
package org.hawkular.metrics.api.jaxrs.util;

/**
 * This is thrown after the session is initialized and it is determined that at least one Cassandra node in the
 * cluster is down. See https://issues.jboss.org/browse/HWKMETRICS-637 for details.
 *
 * @author jsanda
 */
public class CassandraClusterNotUpException extends Exception {

    public CassandraClusterNotUpException() {
        super();
    }

    public CassandraClusterNotUpException(String message) {
        super(message);
    }

    public CassandraClusterNotUpException(String message, Throwable cause) {
        super(message, cause);
    }

    public CassandraClusterNotUpException(Throwable cause) {
        super(cause);
    }

    protected CassandraClusterNotUpException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
