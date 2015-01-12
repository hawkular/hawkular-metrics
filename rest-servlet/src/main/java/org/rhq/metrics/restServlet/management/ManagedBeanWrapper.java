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

import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

/**
 * @author Thomas Segismont
 */
class ManagedBeanWrapper extends StandardMBean {
    private final InterfaceInfo interfaceInfo;

    ManagedBeanWrapper(Object managedBean, InterfaceInfo interfaceInfo) throws NotCompliantMBeanException {
        super(managedBean, interfaceInfo.getInterfaceClass(), interfaceInfo.isMxBean());
        this.interfaceInfo = interfaceInfo;
    }

    @Override
    protected String getDescription(javax.management.MBeanInfo bean) {
        String description = interfaceInfo.getDescription();
        return description.trim().length() > 0 ?  description : super.getDescription(bean);
    }

    @Override
    protected String getDescription(MBeanAttributeInfo op) {
        Set<PropertyInfo> propertyInfos = interfaceInfo.getPropertyInfos();
        PropertyInfo example = PropertyInfo.example(op.getType(), op.getName());
        return propertyInfos.stream()
                .filter(isEqual(example))
                .findFirst()
                .map(PropertyInfo::getDescription)
                .map(description -> description.trim().length() > 0 ? description : super.getDescription(op))
                .orElseThrow(() -> new RuntimeException("Unknown attribute"));
    }

    @Override
    protected String getDescription(MBeanOperationInfo op) {
        return getMethodInfo(op)
                .map(MethodInfo::getDescription)
                .map(description -> description.trim().length() > 0 ?  description : super.getDescription(op))
                .orElseThrow(() -> new RuntimeException("Unknown operation"));
    }

    @Override
    protected int getImpact(MBeanOperationInfo op) {
        return getMethodInfo(op)
                .map(MethodInfo::getImpact)
                .orElseThrow(() -> new RuntimeException("Unknown operation"));
    }

    @Override
    protected String getParameterName(MBeanOperationInfo op, MBeanParameterInfo param, int sequence) {
        return getMethodInfo(op)
                .map(MethodInfo::getParamNames)
                .map(names -> names.get(sequence))
                .map(name -> name.trim().length() > 0 ? name : super.getParameterName(op, param, sequence))
                .orElseThrow(() -> new RuntimeException("Unkown operation"));
    }

    @Override
    protected String getDescription(MBeanOperationInfo op, MBeanParameterInfo param, int sequence) {
        return getMethodInfo(op)
                .map(MethodInfo::getParamDescriptions)
                .map(descriptions -> descriptions.get(sequence))
                .map(desc -> desc.trim().length() > 0 ? desc : super.getDescription(op, param, sequence))
                .orElseThrow(() -> new RuntimeException("Unkown operation"));
    }

    private Optional<MethodInfo> getMethodInfo(MBeanOperationInfo info) {
        List<String> paramTypes = Arrays.stream(info.getSignature())
                .map(MBeanParameterInfo::getType)
                .collect(toList());
        MethodInfo example = MethodInfo.example(info.getName(), paramTypes);
        Set<MethodInfo> methodInfos = interfaceInfo.getMethodInfos();
        return methodInfos
                .stream()
                .filter(isEqual(example))
                .findFirst();
    }
}
