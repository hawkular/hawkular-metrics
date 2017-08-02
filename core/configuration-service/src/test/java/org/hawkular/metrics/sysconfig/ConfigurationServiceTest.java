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
package org.hawkular.metrics.sysconfig;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.hawkular.metrics.schema.SchemaService;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import rx.Observable;
import rx.observers.TestSubscriber;

/**
 * @author jsanda
 */
public class ConfigurationServiceTest {

    private Session session;

    private ConfigurationService configurationService;

    @BeforeClass
    public void initClass() {
        Cluster cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();

        SchemaService schemaService = new SchemaService();
        String keyspace = System.getProperty("keyspace", "hawkulartest");
        schemaService.run(session, keyspace, true);

        configurationService = new ConfigurationService();
        configurationService.init(new RxSessionImpl(session));
    }

    @Test
    public void loadSettings() throws Exception {
        String configurationId = nextConfigurationId();
        insertConfigurationProperty(configurationId, "x", "1");
        insertConfigurationProperty(configurationId, "y", "2");

        List<Configuration> results = getOnNextEvents(() -> configurationService.load(configurationId));
        assertEquals(results.size(), 1);

        Configuration configuration = results.get(0);
        assertEquals(configuration.getProperties(), ImmutableMap.of("x", "1", "y", "2"));
    }

    @Test
    public void updateSettings() {
        String configurationId = nextConfigurationId();
        Map<String, String> properties = ImmutableMap.of("A", "1", "B", "2");
        Configuration configuration = new Configuration(configurationId, properties);

        doAction(() -> configurationService.save(configuration));

        ResultSet resultSet = session.execute("SELECT name, value FROM sys_config WHERE config_id = " +
                "'" + configurationId + "'");
        Map<String, String> actual = new HashMap<>();
        for (Row row : resultSet) {
            actual.put(row.getString(0), row.getString(1));
        }

        assertEquals(actual, properties);
    }

    private String nextConfigurationId() {
        return "org.hawkular.metrics.test." + System.currentTimeMillis();
    }

    private void insertConfigurationProperty(String settingsId, String name, String value) {
        session.execute("INSERT INTO sys_config (config_id, name, value) VALUES ('" + settingsId +
                "', '" + name + "', '" + value + "')");
    }

    /**
     * This method take a function that produces an Observable that has side effects, like
     * inserting rows into the database. A {@link TestSubscriber} is subscribed to the
     * Observable. The subscriber blocks up to five seconds waiting for a terminal event
     * from the Observable.
     *
     * @param fn A function that produces an Observable with side effects
     */
    protected void doAction(Supplier<Observable<Void>> fn) {
        TestSubscriber<Void> subscriber = new TestSubscriber<>();
        Observable<Void> observable = fn.get();
        observable.subscribe(subscriber);
        subscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertCompleted();
    }

    /**
     * This method takes a function that produces an Observable. The method blocks up to
     * five seconds until the Observable emits a terminal event. The items that the
     * Observable emits are then returned.
     *
     * @param fn A function that produces an Observable
     * @param <T> The type of items emitted by the Observable
     * @return A list of the items emitted by the Observable
     */
    protected <T> List<T> getOnNextEvents(Supplier<Observable<T>> fn) {
        TestSubscriber<T> subscriber = new TestSubscriber<>();
        Observable<T> observable = fn.get();
        observable.subscribe(subscriber);
        subscriber.awaitTerminalEvent(5, SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertCompleted();

        return subscriber.getOnNextEvents();
    }

}
