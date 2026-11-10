/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.source;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.NativeInfo;
import io.aiven.kafka.connect.common.source.task.Context;

import org.slf4j.Logger;

/**
 * An abstract source record as retrieved from the storage layer.
 *
 * @param <N>
 *            the native object type.
 * @param <K>
 *            the key type for the native object.
 * @param <T>
 *            the implementation class for AbstractSourceRecord.
 */
public abstract class AbstractSourceRecord<K extends Comparable<K>, N, T extends AbstractSourceRecord<K, N, T>> {
    /** The logger from the implementation class */
    private final Logger logger;
    /** the key for the source record */
    private SchemaAndValue keyData;
    /** The value for the source record. */
    private SchemaAndValue valueData;
    /** The number of records extracted from the native item. Used for checkpoint continuation. */
    private long recordCount;
    /** The context associated with this record */
    private Context<K> context;
    /** The native info for this record. */
    private final NativeInfo<K, N> nativeInfo;

    /**
     * Construct a source record from a native item.
     *
     * @param logger
     *            The logger for the implementation class.
     * @param nativeInfo
     *            the native information for the native that his record represents.
     */
    public AbstractSourceRecord(final Logger logger, final NativeInfo<K, N> nativeInfo) {
        this.logger = logger;
        this.nativeInfo = nativeInfo;
    }

    /**
     * Copy constructor for an abstract source record. This constructor makes a copy of the record count.
     *
     * @param sourceRecord
     *            the source record to copy.
     */
    protected AbstractSourceRecord(final AbstractSourceRecord<K, N, T> sourceRecord) {
        this(sourceRecord.logger, sourceRecord.nativeInfo);
        this.recordCount = sourceRecord.recordCount;
        this.keyData = sourceRecord.keyData;
        this.valueData = sourceRecord.valueData;
        this.context = sourceRecord.context;
    }

    /**
     * Gets then key for the native object.
     *
     * @return The key for the native object.
     */
    final public K getNativeKey() {
        return nativeInfo.getNativeKey();
    }

    /**
     * Gets the number of bytes in the input stream extracted from the native object.
     *
     * @return The number of bytes in the input stream extracted from the native object.
     */
    final public long getNativeItemSize() {
        return nativeInfo.getNativeItemSize();
    }

    /**
     * Makes a duplicate of this AbstractSourceRecord. This is similar to the Java {@code clone} method but without the
     * baggage.
     *
     * @return A duplicate of this AbstractSourceRecord
     */
    abstract public T duplicate();

    /**
     * Gets the native item that this source record is working with.
     *
     * @return The native item that this source record is working with.
     */
    final public N getNativeItem() {
        return nativeInfo.getNativeItem();
    }

    /**
     * Gets the ETag for the native object. Used to detect when a file is overwritten.
     *
     * @return The ETag of the native object. May be {@code null}.
     */
    abstract public String getNativeETag();

    /**
     * Gets the record count for the native item that this source record is working with.
     *
     * @return The record count for the native item that this source record is working with.
     */
    final public long getRecordCount() {
        return recordCount;
    }

    /**
     * Increments the record count for the native item that this source record is working with.
     */
    final public void incrementRecordCount() {
        this.recordCount++;
    }

    /**
     * Sets the number of records that have already been extracted from the native item. Used to resume reading an
     * object from the middle after a restart.
     *
     * @param recordCount
     *            the number of records already extracted.
     */
    final public void setRecordCount(final long recordCount) {
        this.recordCount = recordCount;
    }

    /**
     * Sets the key data for this source record.
     *
     * @param keyData
     *            The key data for this source record.
     */
    final public void setKeyData(final SchemaAndValue keyData) {
        this.keyData = keyData;
    }

    /**
     * Gets the key data for this source record. Makes a defensive copy.
     *
     * @return A copy of the key data for this source record.
     */
    final public SchemaAndValue getKey() {
        return new SchemaAndValue(keyData.schema(), keyData.value());
    }

    /**
     * Sets the value data for this source record.
     *
     * @param valueData
     *            The key data for this source record.
     */
    final public void setValueData(final SchemaAndValue valueData) {
        this.valueData = valueData;
    }

    /**
     * Gets the value data for this source record. Makes a defensive copy.
     *
     * @return A copy of the key data for this source record.
     */
    final public SchemaAndValue getValue() {
        return new SchemaAndValue(valueData.schema(), valueData.value());
    }

    /**
     * Gets the topic for the source record.
     *
     * @return The topic for the source record or {@code null} if it is not set in the context.
     */
    final public String getTopic() {
        return context.getTopic().orElse(null);
    }

    /**
     * Gets the partition for the source record.
     *
     * @return The partition for the source record or {@code null} if it is not set in the context.
     */
    final public Integer getPartition() {
        return context.getPartition().orElse(null);
    }

    /**
     * Gets the Context for this source record. Makes a defensive copy.
     *
     * @return A copy of the Context for this source record.
     */
    final public Context<K> getContext() {
        return new Context<>(context) {
        };
    }

    /**
     * Sets the Context for this source record. Makes a defensive copy.
     *
     * @param context
     *            The Context for this source record.
     */
    final public void setContext(final Context<K> context) {
        this.context = new Context<>(context) {
        };
    }

    /**
     * Converts this source record into a {@link PollerRecord} for the poller output layer.
     *
     * @return a PollerRecord carrying topic/partition/key/value and the record count for checkpoint continuation.
     */
    final public PollerRecord toPollerRecord() {
        if (logger.isDebugEnabled()) {
            logger.debug("Poller Record: {} for Topic: {} , Partition: {}, recordCount: {}", getNativeKey(),
                    getTopic(), getPartition(), getRecordCount());
        }
        return new PollerRecord(getNativeKey().toString(), getNativeETag(), getTopic(), getPartition(), keyData,
                valueData, getRecordCount(), getNativeItemSize());
    }
}
