package org.rhq.metrics.restServlet;

import java.util.Collections;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.rhq.metrics.core.MetricsService;

/**
 * @author John Sanda
 */
@ApplicationScoped
public class MetricsServiceProducer {

    @Inject
    private javax.servlet.ServletContext context;

    private MetricsService metricsService;

    @Produces
    public MetricsService getMetricsService() {
        if (metricsService == null) {
            try {
                String className = context.getInitParameter("rhq-metrics.backend");
                Class clazz = Class.forName(className);
                metricsService = (MetricsService) clazz.newInstance();
                // TODO passs servlet params
                metricsService.startUp(Collections.<String, String>emptyMap());

                ServiceKeeper.getInstance().service = metricsService;

                return metricsService;
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Cannot create MetricsService class", e);
            }
        }
        return metricsService;
    }

}
