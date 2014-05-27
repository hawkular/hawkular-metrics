package org.rhq.metrics.core;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author John Sanda
 */
public class MetricsThreadFactory implements ThreadFactory, Thread.UncaughtExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(MetricsThreadFactory.class);

    private AtomicInteger threadNumber = new AtomicInteger(0);

    private String poolName = "MetricsThreadPool";

    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, poolName + "-" + threadNumber.getAndIncrement());
        t.setDaemon(false);
        t.setUncaughtExceptionHandler(this);

        return t;
    }

    public void uncaughtException(Thread t, Throwable e) {
        logger.error("Uncaught exception on scheduled thread [{}]", t.getName(), e);
    }

}
