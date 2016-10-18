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

package org.hawkular.openshift.auth;

import java.io.InputStream;
import java.util.EventListener;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import io.undertow.servlet.ServletExtension;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.ListenerInfo;
import io.undertow.servlet.util.ImmediateInstanceFactory;

/**
 * An Undertow extension which modifies the handler chain before deployment.
 *
 * @author Thomas Segismont
 */
public class OpenshiftAuthServletExtension implements ServletExtension {
    private OpenshiftAuthHandler openshiftAuthHandler;

    private static final String PROPERTY_FILE = "/WEB-INF/openshift-security-filter.properties";

    @Override
    public void handleDeployment(DeploymentInfo deploymentInfo, ServletContext servletContext) {

        String componentName = "";
        String resourceName = "";
        Pattern insecureEndpointPattern = null;
        Pattern postQueryPattern = null;
        // file to contain configurations options for this Servlet Extension
        InputStream is = servletContext.getResourceAsStream(PROPERTY_FILE);
        if (is != null) {
            try {
                Properties props = new Properties();
                props.load(is);
                componentName = props.getProperty("component");
                resourceName = props.getProperty("resource-name");
                String insecurePatternString = props.getProperty("unsecure-endpoints");
                String postQueryPatternString = props.getProperty("post-query");

                if (insecurePatternString != null) {
                    insecureEndpointPattern = Pattern.compile(insecurePatternString);
                }

                if (postQueryPatternString != null) {
                    postQueryPattern = Pattern.compile(postQueryPatternString);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (componentName == null || componentName.isEmpty()) {
            throw new RuntimeException("Missing or empty 'component' key from the " + PROPERTY_FILE + " configuration file.");
        }

        if (resourceName == null || resourceName.isEmpty()) {
            throw new RuntimeException("Missing or empty 'resource-name' key from the " + PROPERTY_FILE + " configuratin file.");
        }

        final String cName = componentName;
        final String rName = resourceName;
        final Pattern insecurePattern = insecureEndpointPattern;
        final Pattern postQuery = postQueryPattern;

        ImmediateInstanceFactory<EventListener> instanceFactory = new ImmediateInstanceFactory<>(new SCListener());
        deploymentInfo.addListener(new ListenerInfo(SCListener.class, instanceFactory));
        deploymentInfo.addInitialHandlerChainWrapper(containerHandler -> {
            openshiftAuthHandler = new OpenshiftAuthHandler(containerHandler, cName, rName, insecurePattern, postQuery);
            return openshiftAuthHandler;
        });
    }

    private class SCListener implements ServletContextListener {
        @Override
        public void contextInitialized(ServletContextEvent sce) {
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            if (openshiftAuthHandler != null) {
                openshiftAuthHandler.stop();
            }
        }
    }
}
