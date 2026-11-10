/*
 * Copyright 2026 Aiven Oy
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

/**
 * 轮询器输出记录模型，替代 Kafka Connect 的 {@code SourceRecord}。
 *
 * <p>
 * 由 {@link AbstractSourceRecord#toPollerRecord()} 生成，携带一条已解析记录的全部信息：
 * 所属 S3 对象（nativeKey）、目标 topic/partition（可选）、解析出的 key/value 以及
 * 对象内的记录序号（recordCount，用于断点续读）与对象的 eTag（用于检测文件覆盖）。
 * 输出层（file / kafka）只消费本类型，
 * 不再感知 Kafka Connect 的 SourceRecord 概念。
 *
 * <p>
 * 不可变：所有字段构造时一次赋值。
 */
public final class PollerRecord {

    /** 所属 S3 对象的 key，用作断点与日志标识 */
    private final String nativeKey;
    /** 所属 S3 对象的 eTag，断点持久化时记录，用于检测文件被覆盖 */
    private final String nativeETag;
    /** 目标 topic；未设置时为 {@code null} */
    private final String topic;
    /** 目标分区；未设置时为 {@code null} */
    private final Integer partition;
    /** 记录 key（schema + 值），由 Transformer.getKeyData 生成 */
    private final SchemaAndValue keyData;
    /** 记录 value（schema + 值），即解析结果 */
    private final SchemaAndValue valueData;
    /** 该记录在所属对象内的序号（从 1 开始），断点续读时用于 skip */
    private final long recordCount;
    /** 所属对象的字节大小 */
    private final long nativeItemSize;

    /**
     * 构造一条轮询器记录。
     *
     * @param nativeKey
     *            所属 S3 对象的 key。
     * @param nativeETag
     *            所属 S3 对象的 eTag，可为 {@code null}。
     * @param topic
     *            目标 topic，可为 {@code null}。
     * @param partition
     *            目标分区，可为 {@code null}。
     * @param keyData
     *            记录 key。
     * @param valueData
     *            记录 value（解析结果）。
     * @param recordCount
     *            对象内记录序号。
     * @param nativeItemSize
     *            所属对象字节大小。
     */
    public PollerRecord(final String nativeKey, final String nativeETag, final String topic,
            final Integer partition, final SchemaAndValue keyData, final SchemaAndValue valueData,
            final long recordCount, final long nativeItemSize) {
        this.nativeKey = nativeKey;
        this.nativeETag = nativeETag;
        this.topic = topic;
        this.partition = partition;
        this.keyData = keyData;
        this.valueData = valueData;
        this.recordCount = recordCount;
        this.nativeItemSize = nativeItemSize;
    }

    /** @return 所属 S3 对象的 key。 */
    public String nativeKey() {
        return nativeKey;
    }

    /** @return 所属 S3 对象的 eTag，用于检测文件覆盖；可为 {@code null}。 */
    public String nativeETag() {
        return nativeETag;
    }

    /** @return 目标 topic，未设置时为 {@code null}。 */
    public String topic() {
        return topic;
    }

    /** @return 目标分区，未设置时为 {@code null}。 */
    public Integer partition() {
        return partition;
    }

    /** @return 记录 key。 */
    public SchemaAndValue keyData() {
        return keyData;
    }

    /** @return 记录 value（解析结果）。 */
    public SchemaAndValue valueData() {
        return valueData;
    }

    /** @return 对象内记录序号，用于断点续读。 */
    public long recordCount() {
        return recordCount;
    }

    /** @return 所属对象字节大小。 */
    public long nativeItemSize() {
        return nativeItemSize;
    }
}
