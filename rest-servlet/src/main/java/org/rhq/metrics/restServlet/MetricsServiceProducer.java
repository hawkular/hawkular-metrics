package org.rhq.metrics.restServlet;

import java.util.Collections;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.rhq.metrics.core.MetricsService;

/**
 * @author John Sanda
 */
@SuppressWarnings("unused")
@ApplicationScoped
public class MetricsServiceProducer {

/*
    @Inject TODO why does this fail all of a sudden?
    private javax.servlet.ServletContext context;
*/

    private MetricsService metricsService;

    @Produces
    public MetricsService getMetricsService() {
        if (metricsService == null) {
            try {
                String className = null;
                String backend = System.getProperty("rhq-metrics.backend");

                if (backend != null) {
                    switch (backend) {
                    case "mem":
                        className = "org.rhq.metrics.impl.memory.MemoryMetricsService";
                        break;
                    case "cass":
                        className = "org.rhq.metrics.impl.cassandra.MetricsServiceCassandra";
                        break;
                    default:
                        className = "org.rhq.metrics.impl.memory.MemoryMetricsService";

                    }
                }

/*
                if (className == null) {
                    if (context != null) {
                        className = context.getInitParameter("rhq-metrics.backend");
                    }
                }
*/

                if (className == null || className.isEmpty()) {
                    // Fall back to memory backend
                    className = "org.rhq.metrics.impl.memory.MemoryMetricsService";
                }

                System.out.println("Using a backend implementation of " + className);

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
