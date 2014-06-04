/*
 * RHQ Management Platform
 * Copyright (C) 2005-2013 Red Hat, Inc.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA
 */

package org.rhq.metrics.restServlet;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.annotation.WebInitParam;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

/**
 * A filter to wrap json answers as jsonp
 * For this to happen, the user has to pass ?&lt;filter.jsonp.callback>=&lt;name> in the url like
 * <pre>http://localhost:7080/rest/metric/data/10001/raw.json?jsonp=foo</pre>
 * The &lt;filter.jsonp.callback> is defined in web.xml and defaults to 'jsonp'.
 * @author Heiko W. Rupp
 */
@WebFilter(urlPatterns = "/*", asyncSupported = true)
@WebInitParam(name = "filter.jsonp.callback", value = "jsonp", description = "Name of the callback to use for JsonP (?jsonp=...)")
public class JsonPFilter implements Filter {
    private static final String APPLICATION_JSON = "application/json";
    private static final String VND_RHQ_WRAPPED_JSON = "application/vnd.rhq.wrapped+json";
    private static final String ACCEPT = "accept";
    private static final String DEFAULT_CALLBACK_NAME = "jsonp";
    private String callbackName;

    public void destroy() {
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws ServletException,
        IOException {

        if (!(request instanceof HttpServletRequest)) {
            throw new ServletException("This filter can only process HttpServletRequest requests");
        }

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        if (hasCallback(httpRequest)) {

            String callback = getCallback(httpRequest);

            // We need to wrap request and response, as we need to do some re-writing on both
            // We want to get json data inside, so change the accept header
            JsonPRequestWrapper requestWrapper = new JsonPRequestWrapper(httpRequest);
            if (requestsJsonWrapping(httpRequest)) {
                requestWrapper.setHeader(ACCEPT, VND_RHQ_WRAPPED_JSON);
            } else {
                requestWrapper.setHeader(ACCEPT, APPLICATION_JSON);
            }
            requestWrapper.setContentType(APPLICATION_JSON);

            JsonPResponseWrapper responseWrapper = new JsonPResponseWrapper(httpResponse);
            chain.doFilter(requestWrapper, responseWrapper);
            response.setContentType("application/javascript; charset=utf-8");
            ServletOutputStream outputStream = response.getOutputStream();
            outputStream.write((callback + "(").getBytes());
            responseWrapper.getByteArrayOutputStream().writeTo(outputStream);
            outputStream.write(");".getBytes());
            outputStream.flush();

        } else {
            chain.doFilter(request, response);
        }
    }

    /**
     * Check if the incoming request requests jsonw wrapping and jsonp-wrapping
     * @param httpRequest
     * @return
     */
    private boolean requestsJsonWrapping(HttpServletRequest httpRequest) {

        String mimeType = httpRequest.getHeader(ACCEPT);
        if (mimeType.equals(VND_RHQ_WRAPPED_JSON)) {
            return true;
        }

        String localPart = httpRequest.getContextPath();
        if (localPart.endsWith(".jsonw")) {
            return true;
        }

        return false;
    }

    public void init(FilterConfig config) throws ServletException {
        callbackName = config.getInitParameter("filter.jsonp.callback");
        if (callbackName==null) {
            callbackName = DEFAULT_CALLBACK_NAME;
        }
    }

    private boolean hasCallback(ServletRequest request) {
        String cb = request.getParameter(callbackName);
        return (cb != null && !cb.isEmpty());
    }

    private String getCallback(HttpServletRequest request) {
        String parameter = request.getParameter(callbackName);
        if (parameter == null) {
            parameter = DEFAULT_CALLBACK_NAME;
        }
        return parameter;
    }

    private static class JsonPResponseWrapper extends HttpServletResponseWrapper {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        private JsonPResponseWrapper(HttpServletResponse response) {
            super(response);
        }

        ByteArrayOutputStream getByteArrayOutputStream() {
            return baos;
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            return new ServletOutputStream() {
                @Override
                public void write(int b) throws IOException {
                    baos.write(b);
                }
            };
        }

        @Override
        public PrintWriter getWriter() throws IOException {
            return new PrintWriter(baos);
        }
    }

    private static class JsonPRequestWrapper extends HttpServletRequestWrapper {
        int contentLength;
        BufferedReader reader;
        ByteArrayInputStream bais;
        Map<String, String> headers = new HashMap<String, String>();


        public JsonPRequestWrapper(HttpServletRequest request) {
            super(request);
            copyHeaders(request);
        }

        private void copyHeaders(HttpServletRequest request) {

            Enumeration headers = request.getHeaderNames();
            while (headers.hasMoreElements()) {
                String key = (String) headers.nextElement();
                if (key.equalsIgnoreCase("Accept-Encoding")) {
                    // Filter Content codings like compression, as we would end up
                    // with compressed inner data and uncompressed wrapper
                    continue;
                }
                String value = request.getHeader(key);
                this.headers.put(key, value);
            }
        }

        public void setHeader(String key, String value) {
            headers.put(key, value);
        }


        @Override
        public BufferedReader getReader() throws IOException {
            reader = new BufferedReader(new InputStreamReader(bais));
            return reader;
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            return new ServletInputStream() {
                @Override
                public int read() throws IOException {
                    return bais.read();
                }
            };
        }

        private String contentType;

        public void setContentType(String contentType) {
            this.contentType = contentType;
            headers.put("content-type", contentType);
        }

        @Override
        public String getContentType() {
            return contentType;
        }

        @Override
        public int getContentLength() {
            return contentLength;
        }

        @Override
        public String getHeader(String name) {
            String val = headers.get(name);
            if (val != null) {
                return val;
            }
            return super.getHeader(name);
        }

        @Override
        public Enumeration getHeaders(final String name) {
            final String val = headers.get(name);
            return new Enumeration() {
                boolean first = true;

                @Override
                public boolean hasMoreElements() {
                    return first;
                }

                @Override
                public Object nextElement() {
                    if (first) {
                        first = false;
                        return val;
                    } else
                        return null;
                }
            };
        }

        @Override
        public Enumeration getHeaderNames() {
            final Iterator it = headers.keySet().iterator();
            return new Enumeration() {
                public boolean hasMoreElements() {
                    return it.hasNext();
                }

                public Object nextElement() {
                    return it.hasNext() ? it.next() : null;
                }
            };
        }

        @Override
        public int getIntHeader(String name) {
            String val = headers.get(name);
            if (val == null) {
                return 0; // TODO ??
            } else {
                return Integer.parseInt(val);
            }
        }
    }
}
