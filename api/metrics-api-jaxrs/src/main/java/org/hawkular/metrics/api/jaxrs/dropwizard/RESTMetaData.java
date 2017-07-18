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

import org.hawkular.metrics.core.dropwizard.MetaData;

import com.google.common.collect.ImmutableMap;

/**
 * Meta data specific to REST endpoint metrics. Since Hawkular Metrics has some POST endpoints that perform queries, we
 * cannot rely solely on the HTTP verb to determine whether the request is a read or write. Hence the
 * {@link Type} enum.
 *
 * @author jsanda
 */
public class RESTMetaData extends MetaData {

    private static final String SCOPE = "REST";

    public enum Type {
        READ("read"),

        WRITE("write");

        private String text;

        Type(String text) {
            this.text = text;
        }


        @Override
        public String toString() {
            return text;
        }
    }

    private RESTMetricName restMetricName;

    private Type type;


    public static RESTMetaData forRead(HTTPMethod method, String uri, String hostname) {
        return new RESTMetaData(new RESTMetricName(method, uri), Type.READ, hostname);
    }

    public static RESTMetaData forWrite(HTTPMethod method, String uri, String hostname) {
        return new RESTMetaData(new RESTMetricName(method, uri), Type.WRITE, hostname);
    }

    private RESTMetaData(RESTMetricName name, Type type, String hostname) {
        super(name.getName(), SCOPE, type.toString(), hostname, ImmutableMap.of(
                "method", name.getMethod().toString(),
                "uri", name.getUri()
        ));
        this.restMetricName = name;
        this.type = type;
    }

    public RESTMetricName getRESTMetricName() {
        return restMetricName;
    }

    public Type getRequestType() {
        return type;
    }
}
