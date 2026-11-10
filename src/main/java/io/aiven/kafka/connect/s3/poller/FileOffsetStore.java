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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于本地 JSON 文件的断点存储实现，替代 Kafka Connect 的 offset storage。
 *
 * <p>
 * 保存最近一个<b>已完整消费</b>的 S3 对象：bucket / objectKey / recordCount，
 * 同时维护最近 {@code maxProcessedKeys} 个已消费对象的 key→eTag 映射。
 * 进程重启后轮询器把该映射预填进迭代器的环形缓冲与 eTag 表，扫描时
 * 用 key + eTag 联合判断：key 相同但 eTag 变化（文件被覆盖）的对象会重新消费。
 *
 * <p>
 * 写入采用「临时文件 + 原子移动」：先写 {@code <file>.tmp} 再 rename 覆盖，
 * 防止写入中途进程被杀导致偏移文件损坏。
 *
 * <p>
 * 线程安全：读写都由内部锁保护；轮询器单线程顺序消费时不会发生竞争。
 */
public final class FileOffsetStore implements OffsetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileOffsetStore.class);

    private static final String BUCKET = "bucket";
    private static final String OBJECT_KEY = "objectKey";
    private static final String RECORD_COUNT = "recordCount";
    private static final String PROCESSED_KEYS = "processedKeys";

    /** 偏移文件路径 */
    private final Path file;
    /** 已消费 key 列表的最大保留数量 */
    private final int maxProcessedKeys;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object lock = new Object();

    /** 内存中的已消费 key→eTag 映射，插入顺序即消费顺序；与偏移文件保持一致 */
    private final Map<String, String> processedKeys = new LinkedHashMap<>();

    /**
     * 构造断点存储。
     *
     * @param file
     *            偏移文件路径，父目录必须已存在。
     * @param maxProcessedKeys
     *            已消费 key 列表的最大保留数量，超过后丢弃最旧的。
     */
    public FileOffsetStore(final Path file, final int maxProcessedKeys) {
        this.file = file;
        this.maxProcessedKeys = Math.max(1, maxProcessedKeys);
        load().ifPresent(offset -> processedKeys.putAll(offset.processedKeys()));
    }

    /**
     * 读取断点。
     *
     * <p>
     * 文件不存在或内容损坏时返回空（并告警），表示从桶首重新开始消费。
     *
     * @return 最近一次完整消费的偏移；没有则 {@code Optional.empty()}。
     */
    @Override
    public Optional<Offset> load() {
        if (!Files.exists(file)) {
            LOGGER.info("偏移文件不存在，将从桶首开始消费: {}", file);
            return Optional.empty();
        }
        synchronized (lock) {
            try {
                final JsonNode node = mapper.readTree(file.toFile());
                final Map<String, String> keys = new LinkedHashMap<>();
                final JsonNode processed = node.get(PROCESSED_KEYS);
                if (processed != null && processed.isObject()) {
                    // 新格式：key→eTag 映射
                    processed.fields().forEachRemaining(entry -> keys.put(entry.getKey(),
                            entry.getValue().isNull() ? null : entry.getValue().asText()));
                } else if (processed != null && processed.isArray()) {
                    // 旧格式：仅 key 列表，无 eTag 信息，按「已消费」处理（与旧行为一致）
                    processed.forEach(keyNode -> keys.put(keyNode.asText(), null));
                }
                return Optional.of(new Offset(node.path(BUCKET).asText(), node.path(OBJECT_KEY).asText(),
                        node.path(RECORD_COUNT).asLong(0L), keys));
            } catch (final IOException e) {
                LOGGER.warn("读取偏移文件失败，将从桶首重新开始消费: {}", e.getMessage());
                return Optional.empty();
            }
        }
    }

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
     * @throws IllegalStateException
     *             写入失败时抛出，调用方应视为致命错误（断点不准会导致重复消费）。
     */
    @Override
    public void save(final String bucket, final String objectKey, final long recordCount, final String eTag) {
        synchronized (lock) {
            // 维护已消费 key→eTag 映射：去重后追加到尾部，超出上限裁剪最旧的
            processedKeys.remove(objectKey);
            processedKeys.put(objectKey, eTag);
            while (processedKeys.size() > maxProcessedKeys) {
                processedKeys.remove(processedKeys.keySet().iterator().next());
            }
            final Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            try {
                final ObjectNode node = mapper.createObjectNode();
                node.put(BUCKET, bucket);
                node.put(OBJECT_KEY, objectKey);
                node.put(RECORD_COUNT, recordCount);
                node.set(PROCESSED_KEYS, mapper.valueToTree(processedKeys));
                Files.writeString(tmp, mapper.writeValueAsString(node), StandardCharsets.UTF_8);
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (final IOException e) {
                throw new IllegalStateException("保存偏移文件失败: " + file, e);
            }
        }
    }
}
