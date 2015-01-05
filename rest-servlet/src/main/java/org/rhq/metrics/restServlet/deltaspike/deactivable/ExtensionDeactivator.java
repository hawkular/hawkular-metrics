/*
 * Copyright 2014 Red Hat, Inc.
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

package org.rhq.metrics.restServlet.deltaspike.deactivable;

import org.apache.deltaspike.core.impl.jmx.MBeanExtension;
import org.apache.deltaspike.core.spi.activation.ClassDeactivator;
import org.apache.deltaspike.core.spi.activation.Deactivatable;

/**
 * Deltaspike extension deactivator.
 *
 * @author Thomas Segismont
 */
public class ExtensionDeactivator implements ClassDeactivator {

    private static final long serialVersionUID = 1L;

    public Boolean isActivated(Class<? extends Deactivatable> targetClass) {
        // Activate MBeanExtension only
        return MBeanExtension.class.equals(targetClass);
    }

}
