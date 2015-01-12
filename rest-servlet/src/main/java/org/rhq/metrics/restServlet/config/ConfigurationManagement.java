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

package org.rhq.metrics.restServlet.config;

import javax.management.MBeanOperationInfo;
import javax.management.MXBean;

import org.rhq.metrics.restServlet.management.Description;
import org.rhq.metrics.restServlet.management.Impact;
import org.rhq.metrics.restServlet.management.KeyProperty;
import org.rhq.metrics.restServlet.management.MBean;
import org.rhq.metrics.restServlet.management.OperationParamName;

/**
 * @author Thomas Segismont
 */
@MBean(keyProperties = @KeyProperty(key = "type", value = "Configuration"))
@Description("Configuration management")
@MXBean
public interface ConfigurationManagement {
    @Description("Lists the configuration property keys. Some properties may be missing if they were not loaded yet.")
    String[] getConfigurationPropertyKeys();

    @Impact(MBeanOperationInfo.INFO)
    @Description("Get the value of a configuration property")
    String getConfigurationProperty(
        @Description("External form of the configuration property key") @OperationParamName("key") String key);

    @Impact(MBeanOperationInfo.ACTION)
    @Description("Set the value of a configuration property")
    void updateConfigurationProperty(@OperationParamName("key") String key, @OperationParamName("value") String value);
}
