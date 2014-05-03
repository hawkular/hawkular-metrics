package org.rhq.metrics.restServlet;

import org.rhq.metrics.core.MetricsService;

/**
 * Singleton to record the chosen service.
 * @author Heiko W. Rupp
 */
public class ServiceKeeper {
    private static ServiceKeeper ourInstance = new ServiceKeeper();

    public static ServiceKeeper getInstance() {
        return ourInstance;
    }

    private ServiceKeeper() {
    }

    public MetricsService service;
}
