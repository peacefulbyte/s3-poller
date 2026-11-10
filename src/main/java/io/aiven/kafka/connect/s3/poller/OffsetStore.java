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
 * 保存输出确认后的记录进度与对象版本信息。
 *
 * <p>
 * 语义约定（at-least-once）：
 * <ul>
 * <li>保存最近一条已确认记录的 bucket / objectKey / recordCount 及对应 eTag</li>
 * <li>维护最近提交过进度的 key→eTag 映射；最后断点对象可能未完成，必须优先恢复，
 * 其余具有版本信息的条目才可预填为完成对象；版本变化时从头重放</li>
 * <li>{@link #load()} 失败语义由实现决定；{@link #save} 失败应抛异常，调用方视为致命错误
 * （断点不准会导致重复消费）</li>
 * </ul>
 */
public interface OffsetStore {

    /**
     * 读取断点。
     *
     * @return 最近一次已确认的记录断点；没有则 {@code Optional.empty()}。
     */
    Optional<Offset> load();

    /**
     * 批次输出全部确认后保存断点，最后对象可能尚未到 EOF。
     *
     * @param bucket
     *            存储桶名。
     * @param objectKey
     *            本批最后一条已确认记录的对象 key。
     * @param recordCount
     *            该对象内已处理的记录数。
     * @param eTag
     *            该对象的 eTag，用于检测文件覆盖；可为 {@code null}。
     */
    void save(String bucket, String objectKey, long recordCount, String eTag);

    /**
     * 一条断点记录：最后确认的对象及其内部记录数，
     * 附带提交过进度的 key→eTag 映射；最后对象的条目不表示完整消费。
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

        /** 最后断点所在存储桶 */
        public String bucket() {
            return bucket;
        }

        /** 最后断点对象 key，重启必须先恢复它，不能从其后直接扫描 */
        public String objectKey() {
            return objectKey;
        }

        /** 该对象内已处理的记录数（用于对象内部断点续读） */
        public long recordCount() {
            return recordCount;
        }

        /** 提交过进度的 key→eTag 映射；预填完成集合前必须排除最后断点对象 */
        public Map<String, String> processedKeys() {
            return processedKeys;
        }
    }
}
