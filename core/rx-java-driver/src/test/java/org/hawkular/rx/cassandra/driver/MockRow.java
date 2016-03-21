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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.google.common.reflect.TypeToken;

/**
 * @author Thomas Segismont
 */
class MockRow implements Row {
    private final long index;

    MockRow(long index) {
        this.index = index;
    }

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Token getToken(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Token getToken(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Token getPartitionKeyToken() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getTimestamp(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDate getDate(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTime(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getBytesUnsafe(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getBytes(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigInteger getVarint(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getDecimal(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UUID getUUID(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InetAddress getInet(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<T> getList(int i, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UDTValue getUDTValue(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleValue getTupleValue(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(int i, Class<T> targetClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(int i, TypeToken<T> targetType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(int i, TypeCodec<T> codec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getTimestamp(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDate getDate(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTime(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getBytesUnsafe(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getBytes(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigInteger getVarint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getDecimal(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UUID getUUID(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InetAddress getInet(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<T> getList(String name, Class<T> elementsClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<T> getList(String name, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Set<T> getSet(String name, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UDTValue getUDTValue(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleValue getTupleValue(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(String name, Class<T> targetClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(String name, TypeToken<T> targetType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(String name, TypeCodec<T> codec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MockRow mockRow = (MockRow) o;
        return index == mockRow.index;

    }

    @Override
    public int hashCode() {
        return (int) (index ^ (index >>> 32));
    }
}
