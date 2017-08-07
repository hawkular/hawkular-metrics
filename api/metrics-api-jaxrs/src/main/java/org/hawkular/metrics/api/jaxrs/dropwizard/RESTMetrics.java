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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.util.AnnotationLiteral;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

import org.hawkular.metrics.api.jaxrs.util.MetricRegistryProvider;
import org.hawkular.metrics.core.dropwizard.HawkularMetricRegistry;
import org.hawkular.metrics.core.dropwizard.MetricNameService;
import org.jboss.logging.Logger;

import com.codahale.metrics.Timer;

import rx.Observable;

/**
 * Registers metric meta data for REST endpoints. The actual metrics are registered when the end points are invoked for
 * the first time. The metrics here are based on the URI paths with parameters, not the path variables. A client for
 * example might send GET /hawkular/metrics/gauges/MyMetric/raw. The matching metric will be
 * GET gauges/id/raw. The base path of /hawkular/metrics is omitted.
 *
 * @author jsanda
 */
@ApplicationScoped
public class RESTMetrics {

    private static Logger logger = Logger.getLogger(RESTMetrics.class);

    private MetricNameService metricNameService;

    @Inject
    BeanManager beanManager;

    public void setMetricNameService(MetricNameService metricNameService) {
        this.metricNameService = metricNameService;
    }

    public void initMetrics() throws IOException {
        String hostname = metricNameService.getHostName();

        Observable.from(beanManager.getBeans(Object.class, new AnnotationLiteral<Any>() {}))
                .map(Bean::getBeanClass)
                .filter(this::isRESTHandler)
                .flatMap(c -> Observable.from(c.getMethods()))
                .filter(this::isRESTHandlerMethod)
                .subscribe(
                        method -> {
                            HTTPMethod httpMethod = getHttpMethod(method);
                            String uri = getURI(method);
                            if (isWrite(method, uri)) {
                                register(RESTMetaData.forWrite(httpMethod, uri, hostname));
                            } else {
                                register(RESTMetaData.forRead(httpMethod, uri, hostname));
                            }
                        },
                        t -> logger.warn("Failed to register meta data for REST metrics", t),
                        () -> {}
                );
    }

    private boolean isRESTHandler(Class clazz) {
        return clazz.getName().startsWith("org.hawkular.metrics") && (clazz.isAnnotationPresent(Path.class) || Arrays
                .stream(clazz.getMethods()).anyMatch(method -> method.isAnnotationPresent(Path.class)));
    }

    private boolean isRESTHandlerMethod(Method method) {
        return method.isAnnotationPresent(GET.class) ||
                method.isAnnotationPresent(POST.class) ||
                method.isAnnotationPresent(PUT.class) ||
                method.isAnnotationPresent(DELETE.class) ||
                method.isAnnotationPresent(HEAD.class) ||
                method.isAnnotationPresent(OPTIONS.class);
    }

    private boolean isWrite(Method m, String uri) {
        return m.isAnnotationPresent(DELETE.class) || m.isAnnotationPresent(PUT.class) ||
                (m.isAnnotationPresent(POST.class) && !uri.endsWith("query"));
    }

    private HTTPMethod getHttpMethod(Method m) {
        if (m.isAnnotationPresent(GET.class)) {
            return HTTPMethod.GET;
        }
        if (m.isAnnotationPresent(POST.class)) {
            return HTTPMethod.POST;
        }
        if (m.isAnnotationPresent(PUT.class)) {
            return HTTPMethod.PUT;
        }
        if (m.isAnnotationPresent(DELETE.class)) {
            return HTTPMethod.DELETE;
        }
        if (m.isAnnotationPresent(HEAD.class)) {
            return HTTPMethod.HEAD;
        }
        return HTTPMethod.OPTIONS;
    }

    private String getURI(Method method) {
        Path classLevelURI = method.getDeclaringClass().getAnnotation(Path.class);
        Path methodLevelURI = method.getAnnotation(Path.class);

        StringBuilder uri = new StringBuilder();
        if (classLevelURI != null) {
            uri.append(classLevelURI.value());
        }
        if (methodLevelURI != null)
        uri.append(methodLevelURI.value());

        if (uri.charAt(0) == '/') {
            uri.deleteCharAt(0);
        }
        return uri.toString();
    }

    private void register(RESTMetaData metaData) {
        logger.debugf("Registering meta data for %s ", metaData.getRESTMetricName().getName());
        HawkularMetricRegistry registry = MetricRegistryProvider.INSTANCE.getMetricRegistry();
        registry.registerMetaData(metaData);
    }

    public Timer getTimer(String metricName) {
        return MetricRegistryProvider.INSTANCE.getMetricRegistry().timer(metricName);
    }

}
