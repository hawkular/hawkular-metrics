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
package org.hawkular.containers;

import static io.fabric8.kubernetes.api.KubernetesHelper.loadYaml;
import static io.fabric8.kubernetes.api.extensions.Templates.overrideTemplateParameters;
import static io.fabric8.kubernetes.api.extensions.Templates.processTemplatesLocally;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.KubernetesClient;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.openshift.api.model.Template;


/**
 * @author mwringe
 */
public class Util {

    // Default service timeout of 5 minutes
    private static final int SERVICE_TIMEOUT = 300000;

    public static ServiceAccount createServiceAccount(String name) throws Exception {
        ServiceAccount serviceAccount = new ServiceAccount();

        ObjectMeta metadata = new ObjectMeta();
        metadata.setName(name);
        serviceAccount.setMetadata(metadata);

        return serviceAccount;
    }

    public static List<ObjectReference> createSimpleObjectReference(String name) throws Exception {
        ObjectReference objectReference = new ObjectReference();
        objectReference.setName(name);

        List<ObjectReference> secrets = new ArrayList<>();
        secrets.add(objectReference);

        return secrets;
    }

    public static Secret createEmptySecret(String name) throws Exception {
        Secret secret = new Secret();
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setName(name);
        secret.setMetadata(objectMeta);

        return secret;
    }

    public static KubernetesList processTemplate(File templateFile, Map<String, String> properties) throws Exception {
        Template template = (Template)loadYaml(templateFile, Template.class);
        String parameterNamePrefix = "";
        overrideTemplateParameters(template, properties, parameterNamePrefix);
        KubernetesList list = processTemplatesLocally(template, true);
        return list;
    }

    public static void deploy(KubernetesClient client, KubernetesList list) throws Exception {
        for (HasMetadata object: list.getItems()) {
            if (object instanceof Pod) {
                client.createPod((Pod) object);
            } else if (object instanceof Service) {
                client.createService((Service) object);
            } else if (object instanceof ServiceAccount) {
                client.createServiceAccount((ServiceAccount) object);
            } else {
                throw new RuntimeException("Unhandled kubernetes object : " + object.getClass());
            }
        }
    }

    public static void waitForService(KubernetesClient client, String serviceName) throws Exception {
        boolean ready = false;
        long startTime = System.currentTimeMillis();

        System.out.println("WAITING FOR SERVICE '" + serviceName + "' TO START");
        while ((System.currentTimeMillis() - startTime) < SERVICE_TIMEOUT) {
            List<Pod> pods = client.getPodsForService(serviceName);
            if (pods.size() > 0 && pods.get(0).getStatus().getPhase().equalsIgnoreCase("running")) {
                ready = true;
                break;
            }
            // Sleep for a second and then try again.
            Thread.sleep(1000);
        }

        if (!ready) {
            throw new RuntimeException("Service '" + serviceName + "' could not start before the timeout");
        } else {
            System.out.println("SERVICE '" + serviceName + "' HAS STARTED");
        }
    }

}
