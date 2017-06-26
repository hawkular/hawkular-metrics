/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.api;

import java.io.IOException;
import java.io.InputStream;

import org.hawkular.commons.log.MsgLogger;
import org.hawkular.commons.log.MsgLogging;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 *
 * @author Stefan Negrea
 */
public class IspnCacheManager {
    private static final MsgLogger log = MsgLogging.getMsgLogger(IspnCacheManager.class);
    private static final String ISPN_CONFIG_DISTRIBUTED = "ispn.xml";
    private static final String ISPN_CONFIG_LOCAL = "ispn.xml";

    private static EmbeddedCacheManager cacheManager = null;

    public static EmbeddedCacheManager getCacheManager() {
        if (cacheManager == null) {
            init();
        }
        return cacheManager;
    }

    private static synchronized void init() {
        if (cacheManager == null) {
            try {
                InputStream is = null;
                if (is == null) {
                    is = IspnCacheManager.class
                            .getResourceAsStream("/" + ISPN_CONFIG_DISTRIBUTED);
                }
                cacheManager = new DefaultCacheManager(is);
            } catch (IOException e) {
                log.error(e);
            }
        }
    }
}