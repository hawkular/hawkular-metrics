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
package org.rhq.metrics.restServlet;

import javax.inject.Inject;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.rhq.metrics.core.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rest app initialization
 * @author Heiko W. Rupp
 */
@ApplicationPath("/")
public class RHQMetricsRestApp extends Application {

    private static final Logger logger = LoggerFactory.getLogger(RHQMetricsRestApp.class);

    @Inject
    private MetricsService metricsService;

    public RHQMetricsRestApp() {

        logger.info("RHQ Metrics starting ..");

    }

}
