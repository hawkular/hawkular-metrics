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

import static java.util.stream.Collectors.toMap;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * @author Thomas Segismont
 */
class InterfaceInfo {
    private final Class<Object> interfaceClass;
    private final ObjectName objectName;
    private final String description;
    private final boolean mxBean;
    private final Set<PropertyInfo> propertyInfos;
    private final Set<MethodInfo> methodInfos;

    private InterfaceInfo(Class<Object> interfaceClass, ObjectName objectName, String description, boolean mxBean,
        Set<PropertyInfo> propertyInfos, Set<MethodInfo> methodInfos) {
        Objects.requireNonNull(interfaceClass);
        Objects.requireNonNull(objectName);
        Objects.requireNonNull(description);
        Objects.requireNonNull(propertyInfos);
        Objects.requireNonNull(methodInfos);
        this.interfaceClass = interfaceClass;
        this.objectName = objectName;
        this.description = description;
        this.mxBean = mxBean;
        this.propertyInfos = propertyInfos;
        this.methodInfos = methodInfos;
    }

    static InterfaceInfo newIntance(Class<Object> managementInterface) {
        MBean mBean = managementInterface.getAnnotation(MBean.class);
        Description description = managementInterface.getAnnotation(Description.class);
        ObjectName objectName;
        try {
            Map<String, String> keyProperties = Arrays.stream(mBean.keyProperties())
                    .collect(toMap(KeyProperty::key, KeyProperty::value));
            objectName = new ObjectName(mBean.domain(), new Hashtable<>(keyProperties));
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
        boolean mxBean = managementInterface.getAnnotation(MXBean.class) != null;
        BeanInfo javaBean;
        try {
            javaBean = Introspector.getBeanInfo(managementInterface);
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        }
        Set<PropertyInfo> propertyInfos = PropertyInfo.from(javaBean.getPropertyDescriptors());
        Set<MethodInfo> methodInfos = MethodInfo.from(javaBean.getMethodDescriptors());
        return new InterfaceInfo(managementInterface, objectName, description != null ? description.value() : "",
            mxBean, propertyInfos, methodInfos);
    }

    public Class<Object> getInterfaceClass() {
        return interfaceClass;
    }

    public ObjectName getObjectName() {
        return objectName;
    }

    public String getDescription() {
        return description;
    }

    public boolean isMxBean() {
        return mxBean;
    }

    public Set<PropertyInfo> getPropertyInfos() {
        return propertyInfos;
    }

    public Set<MethodInfo> getMethodInfos() {
        return methodInfos;
    }
}
