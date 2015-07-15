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

package org.hawkular.openshift.cassandra;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SeedProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mwringe on 10/04/15.
 */
public class OpenshiftSeedProvider implements SeedProvider {

    private static final Logger logger = LoggerFactory.getLogger(OpenshiftSeedProvider.class);

    private static final String PARAMETER_SEEDS = "seeds";

    private static final String CASSANDRA_NODES_SERVICE_NAME_ENVAR_NAME = "CASSANDRA_NODES_SERVICE_NAME";
    private static final String DEFAULT_CASSANDRA_NODES_SERVICE_NAME = "cassandra-nodes";

    // properties to determine how long to wait for the service to come online.
    // Currently set to 60 seconds
    private static final int SERVICE_TRIES = 30;
    private static final int SERVICE_TRY_WAIT_TIME_MILLISECONDS = 2000;

    //Required Constructor
    public OpenshiftSeedProvider(Map<String, String> args) {
    }

    @Override
    public List<InetAddress> getSeeds() {
        List<InetAddress> seeds = new ArrayList<>();

        String serviceName = getEnv(CASSANDRA_NODES_SERVICE_NAME_ENVAR_NAME, DEFAULT_CASSANDRA_NODES_SERVICE_NAME);
        try {
            InetAddress[] inetAddresses = null;

            // we need to wait until the service is started
            for (int i = 0; i < SERVICE_TRIES; i++) {
                inetAddresses = getInetAddresses(serviceName, inetAddresses, i);
            }

            if (inetAddresses == null) {
                inetAddresses = InetAddress.getAllByName(serviceName);
            }

            logger.debug(inetAddresses.length + " addresses found for service name " + serviceName);

            if (inetAddresses.length >= 1) {
                for (InetAddress inetAddress : inetAddresses) {
                    logger.debug("Adding address " + inetAddress.getHostAddress() + " to seed list");
                    seeds.add(inetAddress);
                }
            } else {
                logger.debug("No hosts found for server '" + serviceName
                        + "' getting this hosts address from the configuration file.");
                seeds = getSeedsFromConfig();
            }
        }
        catch(Exception exception) {
            logger.error("Could not resolve the list of seeds for the Cassandra cluster. Aborting.", exception);
            throw new RuntimeException(exception);
        }

        return seeds;
    }

    private InetAddress[] getInetAddresses(String serviceName, InetAddress[] inetAddresses, int i)
            throws UnknownHostException, InterruptedException {
        try {
            inetAddresses = InetAddress.getAllByName(serviceName);
        } catch (UnknownHostException e) {
            if (i == (SERVICE_TRIES - 1)) {
                logger.error("Could not detect service. Aborting.");
                throw e;
            } else {
                logger.warn("Could not detect service '" + serviceName +
                        "'. It may not be up yet trying again.", e);
                Thread.sleep(SERVICE_TRY_WAIT_TIME_MILLISECONDS);
            }
        }
        return inetAddresses;
    }

    private List<InetAddress> getSeedsFromConfig() throws ConfigurationException {
        List<InetAddress> seedsAddresses = new ArrayList<>();

        Config config = DatabaseDescriptor.loadConfig();
        String seeds = config.seed_provider.parameters.get(PARAMETER_SEEDS);

        for (String seed: seeds.split(",")) {
            try {
                InetAddress inetAddress = InetAddress.getByName(seed);
                logger.debug("Adding seed '" + inetAddress.getHostAddress() + "' from the configuration file.");
                seedsAddresses.add(inetAddress);
            } catch (UnknownHostException e) {
                logger.warn("Could not get address for seed entry '" + seed + "'", e);
            }
        }

        return seedsAddresses;
    }

    private String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value != null) {
            return value;
        } else {
            return defaultValue;
        }
    }
}
