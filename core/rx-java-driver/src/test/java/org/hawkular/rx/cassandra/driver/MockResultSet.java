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

package org.hawkular.rx.cassandra.driver;

import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Thomas Segismont
 */
class MockResultSet implements ResultSet {
    private final long count;
    private final long pageSize;

    private volatile long index;
    private volatile long fetched;

    private MockResultSet(long count, long pageSize) {
        this.count = count;
        this.pageSize = pageSize;
        index = 0;
        fetched = pageSize;
    }

    static MockResultSet createEmpty() {
        return new MockResultSet(0, 0);
    }

    static MockResultSet createSinglePage(long count) {
        Preconditions.checkArgument(count > 0, "No rows");
        return new MockResultSet(count, count);
    }

    static MockResultSet createMultiPage(long count, long pageSize) {
        Preconditions.checkArgument(count > 0, "No rows");
        Preconditions.checkArgument(pageSize > 0, "At least 1 row in a page");
        Preconditions.checkArgument(count > pageSize, "At least 2 pages");
        return new MockResultSet(count, pageSize);
    }

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isExhausted() {
        return index == count;
    }

    @Override
    public Row one() {
        if (isExhausted()) {
            return null;
        }
        if (index == fetched) {
            throw new AssertionError("This method blocks if the next row is not fetched yet");
        }
        return new MockRow(index++);
    }

    @Override
    public List<Row> all() {
        throw new AssertionError("Don't call this blocking method");
    }

    @Override
    public Iterator<Row> iterator() {
        throw new AssertionError("Don't call this method, the iterator may block while fetching");
    }

    @Override
    public int getAvailableWithoutFetching() {
        return (int) Math.max(0, fetched - index);
    }

    @Override
    public boolean isFullyFetched() {
        return fetched == count;
    }

    @Override
    public ListenableFuture<ResultSet> fetchMoreResults() {
        if (fetched < count) {
            fetched += Math.min(pageSize, count - fetched);
        }
        return Futures.immediateFuture(this);
    }

    @Override
    public ExecutionInfo getExecutionInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ExecutionInfo> getAllExecutionInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean wasApplied() {
        throw new UnsupportedOperationException();
    }
}
