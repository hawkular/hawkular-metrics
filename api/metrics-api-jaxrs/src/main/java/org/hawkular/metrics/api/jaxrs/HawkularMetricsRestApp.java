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
package org.hawkular.metrics.api.jaxrs;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.hawkular.metrics.api.jaxrs.exception.mappers.ApplicationExceptionMapper;
import org.hawkular.metrics.api.jaxrs.exception.mappers.BadRequestExceptionMapper;
import org.hawkular.metrics.api.jaxrs.exception.mappers.NotAcceptableExceptionMapper;
import org.hawkular.metrics.api.jaxrs.exception.mappers.NotAllowedExceptionMapper;
import org.hawkular.metrics.api.jaxrs.exception.mappers.NotFoundExceptionMapper;
import org.hawkular.metrics.api.jaxrs.exception.mappers.NotSupportedExceptionMapper;
import org.hawkular.metrics.api.jaxrs.exception.mappers.ReaderExceptionMapper;
import org.hawkular.metrics.api.jaxrs.filter.CorsRequestFilter;
import org.hawkular.metrics.api.jaxrs.filter.CorsResponseFilter;
import org.hawkular.metrics.api.jaxrs.filter.EmptyPayloadFilter;
import org.hawkular.metrics.api.jaxrs.filter.MetricsServiceStateFilter;
import org.hawkular.metrics.api.jaxrs.filter.TenantFilter;
import org.hawkular.metrics.api.jaxrs.handler.AvailabilityHandler;
import org.hawkular.metrics.api.jaxrs.handler.BaseHandler;
import org.hawkular.metrics.api.jaxrs.handler.CounterHandler;
import org.hawkular.metrics.api.jaxrs.handler.GaugeHandler;
import org.hawkular.metrics.api.jaxrs.handler.MetricHandler;
import org.hawkular.metrics.api.jaxrs.handler.PingHandler;
import org.hawkular.metrics.api.jaxrs.handler.PipeHandler;
import org.hawkular.metrics.api.jaxrs.handler.StatusHandler;
import org.hawkular.metrics.api.jaxrs.handler.TenantsHandler;
import org.hawkular.metrics.api.jaxrs.handler.VirtualClockHandler;
import org.hawkular.metrics.api.jaxrs.influx.InfluxSeriesHandler;
import org.hawkular.metrics.api.jaxrs.interceptor.EmptyPayloadInterceptor;
import org.hawkular.metrics.api.jaxrs.log.RestLogger;
import org.hawkular.metrics.api.jaxrs.log.RestLogging;
import org.hawkular.metrics.api.jaxrs.param.ConvertersProvider;
import org.hawkular.metrics.api.jaxrs.util.JacksonConfig;

/**
 * Rest app initialization.
 *
 * @author Heiko W. Rupp
 */
@ApplicationPath("/")
public class HawkularMetricsRestApp extends Application {
    private static final RestLogger log = RestLogging.getRestLogger(HawkularMetricsRestApp.class);

    public HawkularMetricsRestApp() {
        log.infoAppStarting();
    }

    /**
     * The default implementation returns an empty set, which means the JAX-RS runtime will scan the classpath for
     * resources, providers, and filters. When this method is overridden, you have to explicitly include each class.
     * There is no way to simply exclude classes, which is unfortunate because it would make things much easier. We
     * override the method to provide support for testing with a virtual clock through the REST API. The virtual clock
     * endpoint is included and enabled when the hawkular.metrics.use-virtual-clock system property is set to true.
     */
    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> classes = new HashSet<>();

        // Add endpoint handlers
        classes.add(MetricHandler.class);
        classes.add(AvailabilityHandler.class);
        classes.add(InfluxSeriesHandler.class);
        classes.add(TenantsHandler.class);
        classes.add(GaugeHandler.class);
        classes.add(CounterHandler.class);
        classes.add(StatusHandler.class);
        classes.add(BaseHandler.class);
        classes.add(CounterHandler.class);
        classes.add(PingHandler.class);
        classes.add(PipeHandler.class);

        // Initially I tried to inject this using @Configurable and @ConfigurableProperty
        // but null was returned. I assume it has something to do with initialization order.
        // I think it is fine with accessing the system property though since this is only
        // intended for automated tests where we do pass this and other config settings as
        // system properties.
        boolean useVirtualClock = Boolean.valueOf(System.getProperty("hawkular.metrics.use-virtual-clock", "false"));

        if (useVirtualClock) {
            log.infof("Deploying %s", VirtualClockHandler.class.getCanonicalName());
            classes.add(VirtualClockHandler.class);
        } else {
            log.info("Virtual clock is disabled");
        }

        // Add exception mapper providers
        classes.add(BadRequestExceptionMapper.class);
        classes.add(NotAcceptableExceptionMapper.class);
        classes.add(NotAllowedExceptionMapper.class);
        classes.add(NotFoundExceptionMapper.class);
        classes.add(ReaderExceptionMapper.class);
        classes.add(NotSupportedExceptionMapper.class);
        classes.add(ApplicationExceptionMapper.class);

        // Add filters
        classes.add(EmptyPayloadFilter.class);
        classes.add(TenantFilter.class);
        classes.add(MetricsServiceStateFilter.class);
        classes.add(CorsResponseFilter.class);
        classes.add(CorsRequestFilter.class);

        // Add interceptors and other miscellaneous providers
        classes.add(EmptyPayloadInterceptor.class);
        classes.add(ConvertersProvider.class);
        classes.add(org.hawkular.metrics.api.jaxrs.influx.param.ConvertersProvider.class);
        classes.add(JacksonConfig.class);

        return classes;
    }
}
