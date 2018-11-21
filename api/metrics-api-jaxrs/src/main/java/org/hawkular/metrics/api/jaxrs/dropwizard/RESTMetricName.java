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
package org.hawkular.metrics.api.jaxrs.dropwizard;

import java.util.Objects;

/**
 * Metric names of REST endpoints consist of the HTTP method and a relative path, e.g., POST gauges/raw.
 *
 * @author jsanda
 */
public class RESTMetricName {

    private HTTPMethod method;

    private String uri;

    public RESTMetricName(HTTPMethod method, String uri) {
        this.method = method;
        this.uri = uri;
    }

    public HTTPMethod getMethod() {
        return method;
    }

    public String getUri() {
        return uri;
    }

    public String getName() {
        return method.toString() + " " + uri;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RESTMetricName that = (RESTMetricName) o;
        return method == that.method &&
                Objects.equals(uri, that.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, uri);
    }
}
