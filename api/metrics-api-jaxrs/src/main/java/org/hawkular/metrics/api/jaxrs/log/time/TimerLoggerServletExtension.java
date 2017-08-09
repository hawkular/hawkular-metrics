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

import java.io.InputStream;
import java.util.Properties;

import javax.servlet.ServletContext;

import io.undertow.servlet.ServletExtension;
import io.undertow.servlet.api.DeploymentInfo;


public class TimerLoggerServletExtension implements ServletExtension {

    private RequestTimeLogger requestTimeLogger;

    private static final String PROPERTY_FILE = "/WEB-INF/time-logger-filter.properties";


    @Override
    public void handleDeployment(DeploymentInfo deploymentInfo, ServletContext servletContext) {

        long timeThreshold = 0;

        InputStream is = servletContext.getResourceAsStream(PROPERTY_FILE);

        if (is != null) {
            try {
                String timeThresholdString;
                Properties props = new Properties();
                props.load(is);
                timeThresholdString = props.getProperty("logging-time-threshold");
                if (timeThresholdString != null && !timeThresholdString.isEmpty()) {
                    timeThreshold = Long.parseLong(timeThresholdString);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        final long cTimeThreshold = timeThreshold;

        if (timeThreshold > 0) {
            deploymentInfo.addInitialHandlerChainWrapper(containerHandler -> {
                requestTimeLogger = new RequestTimeLogger(containerHandler, cTimeThreshold);
                return requestTimeLogger;
            });
        }
    }
}
