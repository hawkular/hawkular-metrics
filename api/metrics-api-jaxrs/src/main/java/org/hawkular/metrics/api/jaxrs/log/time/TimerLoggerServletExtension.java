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

package org.hawkular.metrics.api.jaxrs.log.time;

import javax.servlet.ServletContext;

import io.undertow.servlet.ServletExtension;
import io.undertow.servlet.api.DeploymentInfo;


public class TimerLoggerServletExtension implements ServletExtension {

    private RequestTimeLogger requestTimeLogger;

    private static final String THRESHOLD_PROPERTY_KEY = "hawkular.metrics.request.logging.time.threshold";
    private static final String THRESHOLD_ENV_VAR = "REQUEST_LOGGING_TIME_THRESHOLD";

    // Threshold units are in ms, 10sec is the default here.
    private static final long DEFAULT_THRESHOLD_VALUE = 10000;

    @Override
    public void handleDeployment(DeploymentInfo deploymentInfo, ServletContext servletContext) {

        long timeThreshold = DEFAULT_THRESHOLD_VALUE;

        String thresholdValue;

        thresholdValue = System.getProperty(THRESHOLD_PROPERTY_KEY);

        if ( thresholdValue == null){
            thresholdValue = System.getenv(THRESHOLD_ENV_VAR);
        }

        if ( thresholdValue != null && !thresholdValue.isEmpty()) {
            try {
                timeThreshold = Long.parseLong(thresholdValue);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        final long cTimeThreshold = timeThreshold;

        deploymentInfo.addInitialHandlerChainWrapper(containerHandler -> {
            requestTimeLogger = new RequestTimeLogger(containerHandler, cTimeThreshold);
            return requestTimeLogger;
        });
    }
}
