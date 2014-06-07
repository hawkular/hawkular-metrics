package org.rhq.metrics.restServlet;

import javax.inject.Inject;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.core.MetricsService;

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
