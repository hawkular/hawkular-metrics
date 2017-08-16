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
package org.hawkular.cassandra.management.commands;

import java.io.IOException;
import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.jboss.logging.Logger;

import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * Runs cleanup for a single table on a single Cassandra node. Required parameters for this command are:
 *
 * <ul>
 *     <li>hostname</li>
 *     <li>jmxPort</li>
 *     <li>keyspace</li>
 *     <li>table</li>
 * </ul>
 *
 * Because there is blocking network I/O involved for the JMX call, subscription is done on  the I/O scheduler.
 *
 * @author jsanda
 */
public class CleanupCommand {

    private static Logger logger = Logger.getLogger(CleanupCommand.class);

    public Observable<Result> run(Map<String, String> context) {
        return Observable.defer(() -> {
            try {
                String hostname = context.get("hostname");
                String jmxPort = context.get("jmxPort");
                String keyspace = context.get("keyspace");
                String table = context.get("table");

                logger.debugf("Running cleanup on %s for %s.%s", hostname, keyspace, table);

                try (JMXConnector connection = createConnection(hostname, jmxPort)) {
                    MBeanServerConnection mbeanServerConnection = connection.getMBeanServerConnection();

                    String storageServiceName = "org.apache.cassandra.db:type=StorageService";
                    String operationName = "forceKeyspaceCleanup";
                    Object[] params = new Object[] {1, keyspace, new String[] {table}};
                    String[] signature = new String[] {int.class.getName(), String.class.getName(),
                            String[].class.getName()};
                    ObjectName mbeanName = new ObjectName("org.apache.cassandra.db:type=StorageService");

                    Object result = mbeanServerConnection.invoke(mbeanName, operationName, params, signature);

                    return Observable.just(new Result(true));
                }
            } catch (Exception e) {
                return Observable.error(e);
            }
        }).subscribeOn(Schedulers.io());
    }

    private JMXConnector createConnection(String hostname, String jmxPort) throws IOException {
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + hostname + ":" + jmxPort + "/jmxrmi");
        return JMXConnectorFactory.connect(url);
    }

}
