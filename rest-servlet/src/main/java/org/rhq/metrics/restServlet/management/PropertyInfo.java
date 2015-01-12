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

import static java.lang.Character.isLowerCase;
import static java.lang.Character.toUpperCase;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Thomas Segismont
 */
class PropertyInfo {
    private final String type;
    private final String name;
    private final String description;

    private PropertyInfo(String type, String name, String description) {
        Objects.requireNonNull(type);
        Objects.requireNonNull(name);
        Objects.requireNonNull(description);
        this.type = type;
        this.name = name;
        this.description = description;
    }

    static PropertyInfo example(String type, String name) {
        return new PropertyInfo(type, name, "");
    }

    static Set<PropertyInfo> from(PropertyDescriptor[] propertyDescriptors) {
        Set<PropertyInfo> infos = new HashSet<>();
        if (propertyDescriptors == null) {
            return infos;
        }
        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            PropertyInfo info = from(propertyDescriptor);
            infos.add(info);
        }
        return infos;
    }

    private static PropertyInfo from(PropertyDescriptor propertyDescriptor) {
        Method method = propertyDescriptor.getReadMethod();
        if (method == null) {
            method = propertyDescriptor.getWriteMethod();
        }
        Description description = method.getAnnotation(Description.class);
        String type = propertyDescriptor.getPropertyType().getName();
        String name = propertyDescriptor.getName();
        if (isLowerCase(name.charAt(0))) {
            // JMX property names must start with an uppercase letter
            name = toUpperCase(name.charAt(0)) + name.substring(1);
        }
        String descriptionText = description != null ? description.value() : "";
        return new PropertyInfo(type, name, descriptionText);
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PropertyInfo that = (PropertyInfo) o;
        return name.equals(that.name) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }
}
