/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.core.service.cache;

import java.io.IOException;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.logging.Logger;

/**
 * @author jsanda
 */
public class CacheServiceImpl implements CacheService {

    private static final Logger logger = Logger.getLogger(CacheServiceImpl.class);

    protected EmbeddedCacheManager cacheManager;

//    AdvancedCache<DataPointKey, DataPoint<? extends Number>> rawDataCache;

    public void init(){
        try {
            logger.info("Initializing caches");
            cacheManager = new DefaultCacheManager(CacheServiceImpl.class.getResourceAsStream(
                    "/metrics-infinispan.xml"));
            cacheManager.startCaches(cacheManager.getCacheNames().toArray(new String[0]));
//            Cache<DataPointKey, DataPoint<? extends Number>> cache = cacheManager.getCache("rawData");
////            rawDataCache = cache.getAdvancedCache();
//            rawDataCache = cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_LOCKING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        logger.info("Shutting down");
        cacheManager.stop();
    }

}
