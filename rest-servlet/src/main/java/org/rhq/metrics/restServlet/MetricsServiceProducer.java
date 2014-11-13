package org.rhq.metrics.restServlet;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.RHQMetrics;
import org.rhq.metrics.core.MetricsService;

/**
 * @author John Sanda
 */
@ApplicationScoped
public class MetricsServiceProducer {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsServiceProducer.class);

    private MetricsService metricsService;

    @Produces
    public MetricsService getMetricsService() {
        if (metricsService == null) {
            String backend = System.getProperty("rhq-metrics.backend");

            RHQMetrics.Builder metricsServiceBuilder = new RHQMetrics.Builder();

            if (backend != null) {
                switch (backend) {
                    case "cass":
                        LOG.info("Using Cassandra backend implementation");
                        metricsServiceBuilder.withCassandraDataStore();
                        break;
                    case "mem":
                    default:
                        LOG.info("Using memory backend implementation");
                        metricsServiceBuilder.withInMemoryDataStore();
                }
            } else {
                metricsServiceBuilder.withInMemoryDataStore();
            }

            metricsService = metricsServiceBuilder.build();
            ServiceKeeper.getInstance().service = metricsService;
        }

        return metricsService;
    }
}
