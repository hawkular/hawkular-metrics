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
package org.hawkular.cassandra.management;

import java.net.InetAddress;

/**
 * The idea for this class is to store the aggregated state of Cassandra cluster events so that it can be used to
 * determine what maintenance to carry out.
 *
 * @author jsanda
 */
public class ClusterState {

    private InetAddress lastNodeAdded;

    public InetAddress getLastNodeAdded() {
        return lastNodeAdded;
    }

    public void setLastNodeAdded(InetAddress lastNodeAdded) {
        this.lastNodeAdded = lastNodeAdded;
    }
}
