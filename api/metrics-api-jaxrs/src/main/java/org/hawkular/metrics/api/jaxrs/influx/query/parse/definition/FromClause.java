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
package org.hawkular.metrics.api.jaxrs.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class FromClause {
    private final String name;
    private final boolean aliased;
    private final String alias;

    public FromClause(String name, String alias) {
        this.name = name;
        this.aliased = (alias != null);
        this.alias = alias;
    }

    public String getName() {
        return name;
    }

    public boolean isAliased() {
        return aliased;
    }

    public String getAlias() {
        return alias;
    }
}
