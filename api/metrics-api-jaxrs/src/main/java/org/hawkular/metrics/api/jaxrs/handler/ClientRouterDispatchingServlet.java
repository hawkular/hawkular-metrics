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
package org.hawkular.metrics.api.jaxrs.handler;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.undertow.servlet.handlers.DefaultServlet;

/**
 * Servlet used for delivering static data as undertow's DefaultServlet and preserves url for client-side router
 * such as Angular's in single-page app context
 * @author Joel Takvorian
 */
public class ClientRouterDispatchingServlet extends DefaultServlet {

    public static final String PATH = "/ui";
    public static final String PATH_INDEX_HTML = PATH + "/index.html";
    private static final String CLIENTSIDE_ROUTE_PREFIX = "/r/";

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        if (req.getPathInfo().startsWith(CLIENTSIDE_ROUTE_PREFIX)) {
            // This is for client-side router, send it to index.html
            req.getRequestDispatcher(PATH_INDEX_HTML).forward(req, resp);
        } else {
            super.doGet(req, resp);
        }
    }
}
