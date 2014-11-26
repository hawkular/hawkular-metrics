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
