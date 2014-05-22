package org.rhq.metrics.restServlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.core.MetricsService;

/**
 * Rest app initalization
 * @author Heiko W. Rupp
 */
@ApplicationPath("/")
public class RHQMetricsRestApp extends Application {

    private static final Logger logger = LoggerFactory.getLogger(RHQMetricsRestApp.class);

    private MetricsService metricsService;

    public RHQMetricsRestApp(@Context ServletConfig servletConfig) {

        logger.info("RHQ Metrics starting ..");

        ServletContext servletContext = servletConfig.getServletContext();
        String backendClassName = servletContext.getInitParameter("rhq-metrics.backend");

        logger.info(".. using a backend of  " + backendClassName);

//        try {
//            @SuppressWarnings("rawtypes")
//            Class clazz = Class.forName(backendClassName);
//
//            metricsService = (MetricsService) clazz.newInstance();
//            Map<String,String> params = new HashMap<>();
//            Enumeration<String> initParameterNames = servletContext.getInitParameterNames();
//            while (initParameterNames.hasMoreElements()) {
//                String name = initParameterNames.nextElement();
//                params.put(name, servletContext.getInitParameter(name));
//            }
//            metricsService.startUp(params);
//        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
//            logger.error("Can not start the backend:", e);
//            throw new RuntimeException("Startup failed");
//        }
//        ServiceKeeper.getInstance().service = metricsService;

    }

    // TODO Figure out why this does not work. Deployment fails with a CDI exception
//    @Produces
//    public MetricsService getMetricsService() {
//        return metricsService;
//    }

}
