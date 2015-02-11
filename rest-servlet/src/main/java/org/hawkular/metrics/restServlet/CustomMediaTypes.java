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
package org.hawkular.metrics.restServlet;

/**
 * Application specific media types.
 *
 * @author Thomas Segismont
 */
public class CustomMediaTypes {

    public static final String APPLICATION_VND_HAWKULAR_WRAPPED_JSON = "application/vnd.hawkular.wrapped+json";

    /**
     * For JSONP.
     * @see org.hawkular.metrics.restServlet.jsonp.JsonPProvider
     */
    public static final String APPLICATION_JAVASCRIPT = "application/javascript";

    private CustomMediaTypes() {
        // Constants class
    }
}
