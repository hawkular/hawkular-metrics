/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.component;

import javax.annotation.PostConstruct;
//import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.core.service.MetricsService;

/**
 * @author Pavol Loffay
 */

// [jshaughn] I'm not sure why this bean should be Eager or ApplicationScoped but as it turns out, these two
// annotations together cause the bind to fail due to "Naming context is read-only" (which is a little crazy for
// java:global).  I'm not sure about the necessity of Eager, so I left it in place, I think we can survive without
// this bean being ApplicationScoped.  I sort of think perhaps neither of these is necessary.
//@ApplicationScoped
@Eager
public class MetricsJNDIPublisher {

    @Inject
    private MetricsService metricsService;

    @PostConstruct
    public void publishMetrics() {
        InitialContext ctx = null;
        try {
            ctx = new InitialContext();
            ctx.bind("java:global/Hawkular/Metrics", metricsService);
        } catch (NamingException e) {
            throw new IllegalStateException("Could not register metrics in JNDI", e);
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (NamingException e) {
                    //ignore
                }
            }
        }
    }
}
