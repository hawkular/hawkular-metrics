/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.clients.ptrans.util;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Thomas Segismont
 */
public class TenantUtil {
    private static final String TENANT_PREFIX = UUID.randomUUID().toString();
    private static final AtomicInteger TENANT_ID_COUNTER = new AtomicInteger(0);

    public static String getRandomTenantId() {
        return "T" + TENANT_PREFIX + TENANT_ID_COUNTER.incrementAndGet();
    }

}
