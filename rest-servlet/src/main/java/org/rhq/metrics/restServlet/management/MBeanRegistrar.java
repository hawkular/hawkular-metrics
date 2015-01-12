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

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Registers an object as a JMX bean.
 * @author Thomas Segismont
 */
@ApplicationScoped
public class MBeanRegistrar {

    private MBeanServer mBeanServer;

    @PostConstruct
    void init() {
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * Registers an object as a JMX Bean.
     * <p>
     *     The object must implement exactly one <em>direct</em> interface annotated with
     *     {@link org.rhq.metrics.restServlet.management.MBean}. This interface will be the management interface exposed
     *     by the object.
     * </p>
     * <strong>The caller must take care of the object unregistration.</strong>
     *
     * @param managedBean the object to register as a JMX bean
     * @see #unregisterMBean(Object)
     */
    public void registerMBean(Object managedBean) {
        Class<Object> managementInterface = getManagementInterface(managedBean);
        InterfaceInfo interfaceInfo = InterfaceInfo.newIntance(managementInterface);
        ObjectName objectName = interfaceInfo.getObjectName();
        unregisterMBean(objectName);
        registerMBean(managedBean, interfaceInfo, objectName);
    }

    private void registerMBean(Object managedBean, InterfaceInfo interfaceInfo, ObjectName objectName) {
        try {
            ManagedBeanWrapper standardMBean = new ManagedBeanWrapper(managedBean, interfaceInfo);
            mBeanServer.registerMBean(standardMBean, objectName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Unregister an object from JMX registry.
     * <p>
     *     The object must implement exactly one <em>direct</em> interface annotated with
     *     {@link org.rhq.metrics.restServlet.management.MBean}.
     * </p>
     *
     * @param managedBean the object to unregister
     */
    public void unregisterMBean(Object managedBean) {
        Class<Object> managementInterface = getManagementInterface(managedBean);
        InterfaceInfo interfaceInfo = InterfaceInfo.newIntance(managementInterface);
        ObjectName objectName = interfaceInfo.getObjectName();
        unregisterMBean(objectName);
    }

    private void unregisterMBean(ObjectName objectName) {
        try {
            if (mBeanServer.isRegistered(objectName)) {
                mBeanServer.unregisterMBean(objectName);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Class<Object> getManagementInterface(Object managedBean) {
        Set<Class<Object>> managementInterfaces = getManagementInterfaces(managedBean);
        if (managementInterfaces.isEmpty()) {
            throw new IllegalArgumentException("Managed bean has not direct management interface");
        }
        if (managementInterfaces.size() > 1) {
            throw new IllegalArgumentException("Managed bean must have a single direct management interface");
        }
        return managementInterfaces.iterator().next();
    }

    private Set<Class<Object>> getManagementInterfaces(Object managedBean) {
        Set<Class<Object>> managementInterfaces = new HashSet<>();
        Class<?>[] beanDirectInterfaces = managedBean.getClass().getInterfaces();
        for (Class<?> beanDirectInterface : beanDirectInterfaces) {
            if (beanDirectInterface.isAnnotationPresent(MBean.class)) {
                @SuppressWarnings("unchecked")
                Class<Object> managementInterface = (Class<Object>) beanDirectInterface;
                managementInterfaces.add(managementInterface);
            }
        }
        return managementInterfaces;
    }

}
