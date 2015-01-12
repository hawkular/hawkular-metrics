/*
 * Copyright 2015 Red Hat, Inc.
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

package org.rhq.metrics.restServlet.management;

import static java.util.stream.Collectors.toList;

import java.beans.MethodDescriptor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.management.MBeanOperationInfo;

/**
 * @author Thomas Segismont
 */
class MethodInfo {
    private final String name;
    private final List<String> paramTypes;
    private final String description;
    private final int impact;
    private final List<String> paramNames;
    private final List<String> paramDescriptions;

    private MethodInfo(String name, List<String> paramTypes, String description, int impact, List<String> paramNames,
        List<String> paramDescriptions) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(paramTypes);
        Objects.requireNonNull(description);
        Objects.requireNonNull(paramNames);
        Objects.requireNonNull(paramDescriptions);
        if (paramNames.size() != paramTypes.size() || paramNames.size() != paramDescriptions.size()) {
            String msg = String.format("names (%s), types (%s) and descriptions (%s) don't have the same size",
                paramNames, paramTypes, paramDescriptions);
            throw new IllegalArgumentException(msg);
        }
        this.name = name;
        this.paramTypes = paramTypes;
        this.description = description;
        this.impact = impact;
        this.paramNames = paramNames;
        this.paramDescriptions = paramDescriptions;
    }

    static MethodInfo example(String name, List<String> paramTypes) {
        return new MethodInfo(name, paramTypes, "", 0, Collections.nCopies(paramTypes.size(), ""), Collections.nCopies(
            paramTypes.size(), ""));
    }

    static Set<MethodInfo> from(MethodDescriptor[] methodDescriptors) {
        Set<MethodInfo> infos = new HashSet<>();
        if (methodDescriptors == null) {
            return infos;
        }
        for (MethodDescriptor methodDescriptor : methodDescriptors) {
            String name = methodDescriptor.getName();
            Method method = methodDescriptor.getMethod();
            Description description = method.getAnnotation(Description.class);
            Impact impact = method.getAnnotation(Impact.class);
            List<String> paramTypes = Arrays.stream(method.getParameterTypes())
                    .map(Class::getName)
                    .collect(toList());
            List<String> paramNames = Arrays.stream(method.getParameters())
                    .map(p -> {
                        OperationParamName pName = p.getAnnotation(OperationParamName.class);
                        return pName != null ? pName.value() : "";
                    })
                    .collect(toList());
            List<String> paramDescriptions = Arrays.stream(method.getParameters()).map(p -> {
                Description pDescription = p.getAnnotation(Description.class);
                return pDescription != null ? pDescription.value() : "";
            }).collect(toList());
            MethodInfo info = new MethodInfo(name, paramTypes, description != null ? description.value() : "",
                impact != null ? impact.value() : MBeanOperationInfo.UNKNOWN, paramNames, paramDescriptions);
            infos.add(info);
        }
        return infos;
    }

    public String getName() {
        return name;
    }

    public List<String> getParamTypes() {
        return paramTypes;
    }

    public String getDescription() {
        return description;
    }

    public int getImpact() {
        return impact;
    }

    public List<String> getParamNames() {
        return paramNames;
    }

    public List<String> getParamDescriptions() {
        return paramDescriptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MethodInfo that = (MethodInfo) o;
        return name.equals(that.name) && paramTypes.equals(that.paramTypes);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + paramTypes.hashCode();
        return result;
    }
}
