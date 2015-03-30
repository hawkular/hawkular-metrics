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
package org.hawkular.metrics.api.jaxrs.service.processor;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.lang.model.element.ExecutableElement;
import javax.ws.rs.container.AsyncResponse;

/**
 * @author Thomas Segismont
 */
public final class MethodModel {
    private final String name;
    private final List<ArgModel> args;
    private final List<ArgModel> filteredArgs;
    private final ArgModel asyncResponseArg;

    public MethodModel(ExecutableElement element) {
        name = element.getSimpleName().toString();
        List<ArgModel> args = element.getParameters().stream()
                                     .map(ArgModel::new)
                                     .collect(toList());
        this.args = Collections.unmodifiableList(args);
        List<ArgModel> filteredArgs;
        filteredArgs = args.stream()
                           .filter(argModel -> !AsyncResponse.class.getName().equals(argModel.getType()))
                           .collect(toList());
        this.filteredArgs = Collections.unmodifiableList(filteredArgs);
        List<ArgModel> temp = new ArrayList<>(args);
        temp.removeAll(filteredArgs);
        asyncResponseArg = temp.get(0);
    }

    public String getName() {
        return name;
    }

    public List<ArgModel> getArgs() {
        return args;
    }

    public List<ArgModel> getFilteredArgs() {
        return filteredArgs;
    }

    public ArgModel getAsyncResponseArg() {
        return asyncResponseArg;
    }
}
