/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.dropwizard;

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.hawkular.metrics.core.service.log.CoreLogger;
import org.hawkular.metrics.core.service.log.CoreLogging;

import com.codahale.metrics.ObjectNameFactory;

/**
 * @author ruben vargas
 */
public class HawkularObjectNameFactory implements ObjectNameFactory {
    private static final CoreLogger LOGGER = CoreLogging.getCoreLogger(HawkularObjectNameFactory.class);

    private HawkularMetricRegistry metricRegistry;

    public HawkularObjectNameFactory(HawkularMetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    private boolean needsQuote(String propertyValue){
        String[] specialCharacters = {"*","?","=",":",","};
        for (String c: specialCharacters){
            if (propertyValue.contains(c)) return true;
        }
        return false;
    }

    @Override
    public ObjectName createName(String type, String domain, String name) {
        try {
            Hashtable<String, String> table = new Hashtable<>();
            MetaData metaData = metricRegistry.getMetaData(name);
            if (metaData == null) {
                LOGGER.warnf("No meta data found for %s", name);
                return new ObjectName(domain, type, name);
            } else {
                table.putAll(metricRegistry.getMetaData(name).getTags());
                table.replaceAll((k,v) -> needsQuote(v) ? ObjectName.quote(v): v );
                return new ObjectName(domain, table);
            }
        } catch (MalformedObjectNameException e) {
            LOGGER.warn("Unable to register {" + type + "} {" + name + "}", e);
            throw new RuntimeException(e);
        }
    }
}
