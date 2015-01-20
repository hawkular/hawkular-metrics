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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class MBeanRegistrarTest {

    private MBeanRegistrar mBeanRegistrar;
    private Object sample;
    private MBeanServer mBeanServer;

    @Before
    public void before() {
        mBeanRegistrar = new MBeanRegistrar();
        mBeanRegistrar.init();
        mBeanServer = mBeanRegistrar.mBeanServer;
        sample = new ManageableSample();
    }

    @Test
    public void testRegisterMBean() throws Exception {
        mBeanRegistrar.registerMBean(sample);
        MBeanInfo mBean = mBeanServer.getMBeanInfo(new ObjectName("com.marseille:mare=nostrum"));
        assertThat(mBean).isNotNull();
        assertThat(mBean.getDescription()).isNotNull().isEqualTo("SampleManagementDescription");

        MBeanAttributeInfo[] attributes = mBean.getAttributes();
        assertThat(attributes).isNotNull().hasSize(2);
        for (MBeanAttributeInfo attribute : attributes) {
            switch (attribute.getName()) {
            case "ReadWriteAttribute":
                assertThat(attribute.getDescription()).isNotNull().isEqualTo("Description from RW getter method");
                break;
            case "WriteOnlyAttribute":
                assertThat(attribute.getDescription()).isNotNull().isEqualTo("Description from W setter method");
                break;
            default:
                fail("Unexpected attribute '" + attribute.getName() + "'");
            }
        }

        MBeanOperationInfo[] operations = mBean.getOperations();
        assertThat(operations).isNotNull().hasSize(2);
        for (MBeanOperationInfo operation : operations) {
            switch (operation.getName()) {
            case "info":
                assertThat(operation.getDescription()).isNotNull().isEqualTo("Description info");
                assertThat(operation.getImpact()).isEqualTo(MBeanOperationInfo.INFO);
                MBeanParameterInfo[] parameters = operation.getSignature();
                assertThat(parameters).isNotNull().hasSize(1);
                MBeanParameterInfo parameter = parameters[0];
                assertThat(parameter.getName()).isNotNull().isEqualTo("marseille");
                assertThat(parameter.getDescription()).isNotNull().isEqualTo("Description param");
                break;
            case "unknown":
                assertThat(operation.getDescription()).isNotNull().isEqualTo("Description unknown");
                assertThat(operation.getImpact()).isEqualTo(MBeanOperationInfo.UNKNOWN);
                break;
            default:
                fail("Unexpected operation '" + operation.getName() + "'");
            }
        }
    }

    @After
    public void after() {
        mBeanRegistrar.unregisterMBean(sample);
    }

    @MBean(domain = "com.marseille", keyProperties = @KeyProperty(key = "mare", value = "nostrum"))
    @Description("SampleManagementDescription")
    public interface SampleManagement {
        @Description("Description from RW getter method")
        int[] getReadWriteAttribute();

        @Description("Description from RW setter method")
        void setReadWriteAttribute(int[] value);

        @Description("Description from W setter method")
        void setWriteOnlyAttribute(String value);

        @Description("Description info")
        @Impact(MBeanOperationInfo.INFO)
        void info(@OperationParamName("marseille") @Description("Description param") String param);

        @Description("Description unknown")
        // Unspecified impact
        int unknown(String param);
    }

    private class ManageableSample implements SampleManagement {
        @Override
        public int[] getReadWriteAttribute() {
            return new int[0];
        }

        @Override
        public void setReadWriteAttribute(int[] value) {
        }

        @Override
        public void setWriteOnlyAttribute(String value) {
        }

        @Override
        public void info(String param) {
        }

        @Override
        public int unknown(String param) {
            return 0;
        }
    }
}
