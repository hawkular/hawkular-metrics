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
package org.hawkular.metrics.tasks.api;

import java.util.concurrent.TimeUnit;

/**
 * @author jsanda
 */
public class TriggerBuilder {

    private class RepeatingTriggerBuilder {

        public RepeatingTriggerBuilder withInterval(int interval, TimeUnit timeUnit) {
            return this;
        }

        public RepeatingTriggerBuilder withDelay(int delay, TimeUnit timeUnit) {
            return this;
        }

        public RepeatingTriggerBuilder withRepeatCount(int count) {
            return this;
        }

        public Trigger build() {
//            return new RepeatingTrigger(0);
            return null;
        }
    }

    private class SingleTriggerBuilder {

        public SingleTriggerBuilder withDelay(long delay, TimeUnit timeUnit) {
            return this;
        }

        public SingleTriggerBuilder withTriggerTime(long time) {
            return this;
        }

        public Trigger build() {
            return new SingleExecutionTrigger(0);
        }
    }

}
