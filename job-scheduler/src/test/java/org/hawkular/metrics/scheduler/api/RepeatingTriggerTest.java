/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.scheduler.api;

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

/**
 * @author jsanda
 */
public class RepeatingTriggerTest {

    @Test
    public void createTriggerWithNoParams() {
        RepeatingTrigger trigger = new RepeatingTrigger.Builder().build();
        assertEquals(trigger.getTriggerTime(), currentMinute().plusMinutes(1).getMillis());
        assertEquals(trigger.nextTrigger().getTriggerTime(), currentMinute().plusMinutes(2).getMillis());
    }

    @Test
    public void createTriggerWithTwoHourInterval() {
        RepeatingTrigger trigger = new RepeatingTrigger.Builder().withInterval(2, TimeUnit.HOURS).build();
        assertEquals(trigger.getTriggerTime(), currentMinute().plusMinutes(1).getMillis());
        assertEquals(trigger.nextTrigger().getTriggerTime(), currentMinute().plusMinutes(1).plusHours(2).getMillis());
    }

    @Test
    public void createTriggerWithDelayAndInterval() {
        RepeatingTrigger trigger = new RepeatingTrigger.Builder()
                .withDelay(10, TimeUnit.MINUTES)
                .withInterval(3, TimeUnit.DAYS)
                .build();
        assertEquals(trigger.getTriggerTime(), currentMinute().plusMinutes(10).getMillis());
        assertEquals(trigger.nextTrigger().getTriggerTime(), currentMinute().plusMinutes(10).plusDays(3).getMillis());
    }

    @Test
    public void createTriggerWithRepeatCount() {
        RepeatingTrigger trigger = new RepeatingTrigger.Builder().withRepeatCount(2).build();
        assertEquals(trigger.getTriggerTime(), currentMinute().plusMinutes(1).getMillis());
        assertEquals(trigger.nextTrigger().getTriggerTime(), currentMinute().plusMinutes(2).getMillis());
        assertNull(trigger.nextTrigger().nextTrigger());
    }

    @Test
    public void createTriggerWithTriggerTimeAndInterval() {
        RepeatingTrigger trigger = new RepeatingTrigger.Builder()
                .withInterval(5, TimeUnit.MINUTES)
                .withTriggerTime(currentMinute().plusMinutes(2).getMillis())
                .build();
        assertEquals(trigger.getTriggerTime(), currentMinute().plusMinutes(2).getMillis());
        assertEquals(trigger.nextTrigger().getTriggerTime(), currentMinute().plusMinutes(7).getMillis());
    }

}
