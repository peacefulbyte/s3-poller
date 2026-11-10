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

package io.aiven.kafka.connect.s3.poller;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * 断点存储抽象，替代 Kafka Connect 的 offset storage。
 *
 * <p>
 * 语义约定（at-least-once）：
 * <ul>
 * <li>保存最近一个<b>已完整消费</b>的 S3 对象：bucket / objectKey / recordCount / eTag</li>
 * <li>同时维护最近 {@code maxProcessedKeys} 个已消费对象的 key→eTag 映射，
 * 重启后预填迭代器的环形缓冲与 eTag 表，key 相同但 eTag 不同（文件被覆盖）时重新消费</li>
 * <li>{@link #load()} 失败语义由实现决定；{@link #save} 失败应抛异常，调用方视为致命错误
 * （断点不准会导致重复消费）</li>
 * </ul>
 */
public interface OffsetStore {

    /**
     * 读取断点。
     *
     * @return 最近一次完整消费的偏移；没有则 {@code Optional.empty()}。
     */
    Optional<Offset> load();

    /**
     * 保存断点。一个对象完整消费完后调用，用于下次重启续读。
     *
     * @param bucket
     *            存储桶名。
     * @param objectKey
     *            已完整消费的对象 key。
     * @param recordCount
     *            该对象内已处理的记录数。
     * @param eTag
     *            该对象的 eTag，用于检测文件覆盖；可为 {@code null}。
     */
    void save(String bucket, String objectKey, long recordCount, String eTag);

    /**
     * 一条断点记录：最近完整消费的 S3 对象及其内部已处理记录数，
     * 附带最近已完整消费的对象 key→eTag 映射（尾部为最新）。
     */
    final class Offset {
        private final String bucket;
        private final String objectKey;
        private final long recordCount;
        private final Map<String, String> processedKeys;

        public Offset(final String bucket, final String objectKey, final long recordCount,
                final Map<String, String> processedKeys) {
            this.bucket = bucket;
            this.objectKey = objectKey;
            this.recordCount = recordCount;
            this.processedKeys = Collections.unmodifiableMap(processedKeys);
        }

        /** 已完整消费的对象所在存储桶 */
        public String bucket() {
            return bucket;
        }

        /** 已完整消费的对象 key，重启后从此 key 之后继续扫描 */
        public String objectKey() {
            return objectKey;
        }

        /** 该对象内已处理的记录数（用于对象内部断点续读） */
        public long recordCount() {
            return recordCount;
        }

        /** 最近已完整消费的对象 key→eTag 映射，尾部为最新；重启后用于预填去重缓冲与 eTag 表 */
        public Map<String, String> processedKeys() {
            return processedKeys;
        }
    }
}
