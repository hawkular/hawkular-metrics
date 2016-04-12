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

import java.util.EventListener;

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

    @Override
    public void handleDeployment(DeploymentInfo deploymentInfo, ServletContext servletContext) {
        ImmediateInstanceFactory<EventListener> instanceFactory = new ImmediateInstanceFactory<>(new SCListener());
        deploymentInfo.addListener(new ListenerInfo(SCListener.class, instanceFactory));
        deploymentInfo.addInitialHandlerChainWrapper(containerHandler -> {
            openshiftAuthHandler = new OpenshiftAuthHandler(containerHandler);
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