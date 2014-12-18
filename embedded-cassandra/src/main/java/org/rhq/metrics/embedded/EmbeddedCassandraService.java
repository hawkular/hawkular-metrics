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
package org.rhq.metrics.embedded;

import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Schedule;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.annotation.PreDestroy;
import javax.annotation.PostConstruct;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * App initialization
 *
 * @author Stefan Negrea
 */
@Startup
@Singleton
public class EmbeddedCassandraService {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedCassandraService.class);

    private CassandraDaemon cassandraDaemon;

    public EmbeddedCassandraService() {
        logger.info("======== RHQ Metrics - Embedded Cassandra ========");
    }

    @PostConstruct
    public void start() {
        synchronized (this) {
            if(cassandraDaemon == null) {
                try {
                    ConfigEditor editor = new ConfigEditor();
                    editor.initEmbeddedConfiguration();

                    cassandraDaemon = new CassandraDaemon();
                    cassandraDaemon.activate();
                } catch (Exception e) {
                    logger.error("Error initializing embbeded Cassandra server", e);
                }
            }
        }
    }

    @PreDestroy
    void stop() {
        synchronized (this) {
            if(cassandraDaemon != null) {
                cassandraDaemon.deactivate();
            }
        }
    }

    @Schedule(second="*/6", minute="*",hour="*", persistent=false)
    public void doWork(){
        Date currentTime = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd G 'at' HH:mm:ss z");
        logger.error( "ScheduleExample.doWork() invoked at " + simpleDateFormat.format(currentTime) );
    }
}
