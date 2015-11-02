package org.hawkular.metrics.core.impl;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

public class TestCacheManager {
    private static EmbeddedCacheManager cacheManager;

    static {
        cacheManager = new DefaultCacheManager();
    }

    public static synchronized EmbeddedCacheManager getCacheManager() {
        return cacheManager;
    }
}
