/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.rhq.metrics.impl.embedded;

import org.apache.cassandra.service.CassandraDaemon;
import org.rhq.metrics.impl.cassandra.MetricsServiceCassandra;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Stefan Negrea
 */
public class MetricsServiceEmbeddedCassandra extends MetricsServiceCassandra {

    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceEmbeddedCassandra.class);

    private CassandraDaemon cassandraDaemon;

    @Override
    public void startUp(java.util.Map<String, String> params) {
        try {
            ConfigEditor editor = new ConfigEditor();
            editor.initEmbeddedConfiguration();

            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.activate();
        } catch (Exception e) {
            logger.error("Error initializing embbeded Cassandra server", e);
        }

        super.startUp(params);
    };

    @Override
    public void shutdown() {
        super.shutdown();

        cassandraDaemon.deactivate();
    };
}