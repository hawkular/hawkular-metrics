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
package org.hawkular.metrics.clients.ptrans.matchers;

import java.io.File;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * @author Thomas Segismont
 */
public class CanReadMatcher extends TypeSafeMatcher<File> {
    @Override
    public void describeTo(Description description) {
        description.appendText("can read");
    }

    @Override
    protected void describeMismatchSafely(File item, Description mismatchDescription) {
        mismatchDescription.appendValue(item).appendText(" is not readable");
    }

    @Override
    protected boolean matchesSafely(File item) {
        return item.canRead();
    }

    @Factory
    public static Matcher<File> canRead() {
        return new CanReadMatcher();
    }
}
