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
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.hawkular.metrics.api.jaxrs.util.MetricRegistryProvider;
import org.hawkular.metrics.core.dropwizard.HawkularMetricRegistry;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.common.TextFormat;

@Path("/prometheus")
@GZIP
@ApplicationScoped
public class PrometheusHandler {

    static final HawkularMetricRegistry metricRegistry = MetricRegistryProvider.INSTANCE.getMetricRegistry();

    CollectorRegistry prometheusRegistry;
    DropwizardExports dropwizardCollector;

    @PostConstruct
    public void init(){
        prometheusRegistry = new CollectorRegistry(true);
        dropwizardCollector = new DropwizardExports(metricRegistry);
        dropwizardCollector.register(prometheusRegistry);
    }

    @GET
    @Produces(TextFormat.CONTENT_TYPE_004)
    public Response getMetrics() throws IOException{
        HttpServletRequest request = ResteasyProviderFactory.getContextData(HttpServletRequest.class);
        StringWriter writer = new StringWriter();
        TextFormat.write004(writer, prometheusRegistry.filteredMetricFamilySamples(parse(request)));
        writer.flush();
        return Response.ok(writer.toString()).build();
    }

    private Set<String> parse(HttpServletRequest req) {
        String[] includedParam = req.getParameterValues("name[]");
        if (includedParam == null) {
            return Collections.emptySet();
        } else {
            return new HashSet<>(Arrays.asList(includedParam));
        }
    }
}
