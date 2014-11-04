package org.rhq.metrics.restServlet;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.rhq.metrics.RHQMetrics;
import org.rhq.metrics.core.MetricsService;

/**
 * @author John Sanda
 */
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
            String backend = System.getProperty("rhq-metrics.backend");

            RHQMetrics.Builder metricsServiceBuilder = new RHQMetrics.Builder();

            if (backend != null) {
                switch (backend) {
                    case "mem":
                        metricsServiceBuilder.withInMemoryDataStore();
                        break;
                    case "cass":
                        metricsServiceBuilder.withCassandraDataStore();
                        break;
                    default:
                        metricsServiceBuilder.withInMemoryDataStore();
                }
            } else {
                metricsServiceBuilder.withInMemoryDataStore();
            }

            System.out.println("Using a backend implementation of " + backend);

            metricsService = metricsServiceBuilder.build();
            ServiceKeeper.getInstance().service = metricsService;
        }

        return metricsService;
    }
}
