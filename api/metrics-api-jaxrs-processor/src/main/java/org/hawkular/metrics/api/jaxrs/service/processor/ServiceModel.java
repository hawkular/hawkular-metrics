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
package org.hawkular.metrics.api.jaxrs.service.processor;

import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.List;

import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;

/**
 * @author Thomas Segismont
 */
public final class ServiceModel {
    private final String packageName;
    private final String serviceName;
    private final String serviceClass;
    private final List<MethodModel> methods;

    public ServiceModel(TypeElement element) {
        packageName = evalPackageName(element);
        serviceName = element.getSimpleName().toString();
        serviceClass = element.getQualifiedName().toString();
        List<MethodModel> methods = element.getEnclosedElements().stream()
                                           .filter(enclosed -> enclosed.getKind() == ElementKind.METHOD)
                                           .map(ExecutableElement.class::cast)
                                           .map(enclosed -> new MethodModel(enclosed))
                                           .collect(toList());
        this.methods = Collections.unmodifiableList(methods);
    }

    private String evalPackageName(TypeElement element) {
        Element enclosingElement = element.getEnclosingElement();
        if (!(enclosingElement instanceof PackageElement)) {
            throw new IllegalArgumentException(element + " is a nested interface");
        }
        PackageElement packageElement = (PackageElement) enclosingElement;
        return packageElement.getQualifiedName().toString();
    }

    public String getPackageName() {
        return packageName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getServiceClass() {
        return serviceClass;
    }

    public List<MethodModel> getMethods() {
        return methods;
    }
}
