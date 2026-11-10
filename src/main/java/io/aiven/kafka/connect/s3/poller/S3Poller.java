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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aiven.kafka.connect.common.config.CommonConfig;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.PollerRecord;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.AWSV2SourceClient;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecordIterator;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 独立 S3 轮询器核心，把「扫描 → 下载 → 解析 → 输出 → 断点持久化」串成循环。
 *
 * <p>
 * 沿用旧 {@link S3SourceConfig} 配置链路驱动 {@link S3SourceRecordIterator}
 * （connect-api 暂保留仅作数据类依赖），新 YAML 配置通过 {@link #toSourceProps(PollerConfig)}
 * 映射为旧属性 key；输出层与断点存储使用 poller 包新实现。
 *
 * <p>
 * 断点语义（at-least-once）：
 * <ul>
 * <li>每轮轮询结束时先 {@link OutputSink#flush()} 确认送达，成功才推进断点；
 * Kafka 发送失败时 flush 抛 IOException，断点不推进，重启后整批重发</li>
 * <li>断点维护最近提交进度的 key→eTag 映射；重启先恢复最后对象，其余完成条目
 * 预填去重缓冲，版本变化或缺失时保守重放</li>
 * <li>单轮输出条数受 {@code poll.maxPollRecords} 限制，防止持续有新对象时
 * 断点长时间得不到持久化</li>
 * </ul>
 */
public final class S3Poller implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3Poller.class);

    /** filePattern 未配置时的兜底模板：FilePatternUtils 要求非空，正则 .* 匹配所有对象 */
    static final String MATCH_ALL_PATTERN = ".*";

    private final PollerConfig config;
    private final OffsetStore offsetStore;
    private final OutputSink sink;
    private final S3SourceRecordIterator iterator;
    private final AWSV2SourceClient s3Client;
    private final String bucket;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private S3Poller(final PollerConfig config, final OffsetStore offsetStore, final OutputSink sink,
            final S3SourceRecordIterator iterator, final AWSV2SourceClient s3Client, final String bucket) {
        this.config = config;
        this.offsetStore = offsetStore;
        this.sink = sink;
        this.iterator = iterator;
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    /**
     * 按配置组装轮询器：旧配置映射 → S3 客户端 → 断点存储 → 迭代器 → 输出层。
     * S3 恢复与扫描均为懒加载；MySQL 断点存储会在组装时访问数据库。
     *
     * @param config
     *            已校验的 YAML 配置。
     * @return 就绪的轮询器。
     */
    public static S3Poller create(final PollerConfig config) {
        config.validate();
        final S3SourceConfig s3SourceConfig = new S3SourceConfig(toSourceProps(config));
        // 临时文件下载目录：为空时交给 AWSV2SourceClient 使用系统临时目录
        final Path downloadDir = StringUtils.isNotBlank(config.poll.downloadDir)
                ? Path.of(config.poll.downloadDir)
                : null;
        // path-style 按存储服务选择：AWS/腾讯云 COS 用 virtual-hosted-style，MinIO 等本地服务用 path-style
        final AWSV2SourceClient s3Client = new AWSV2SourceClient(s3SourceConfig, downloadDir,
                config.s3.pathStyleAccess);
        // 断点存储按 offsets.type 选择：file=本地 JSON 文件 / mysql=MySQL 表
        final OffsetStore offsetStore = config.offsets.type().equalsIgnoreCase("mysql")
                ? MySqlOffsetStore.create(config)
                : new FileOffsetStore(Path.of(config.offsets.file), config.poll.ringBufferSize);
        final OutputSink sink = config.output.type() == PollerConfig.OutputType.FILE
                ? new FileOutputSink(config.output.file, config.inputFormat())
                : new KafkaOutputSink(config.output.kafka, config.inputFormat());
        return create(config, offsetStore, sink, s3Client);
    }

    /** 生产与故障注入验证共用的组装入口。 */
    static S3Poller create(final PollerConfig config, final OffsetStore offsetStore, final OutputSink sink,
            final AWSV2SourceClient s3Client) {
        config.validate();
        final S3SourceConfig sourceConfig = new S3SourceConfig(toSourceProps(config));
        final Optional<OffsetStore.Offset> offset = offsetStore.load()
                .filter(o -> config.s3.bucket.equals(o.bucket()));
        final Map<String, String> completedKeys = new LinkedHashMap<>();
        offset.ifPresent(o -> o.processedKeys().forEach((key, eTag) -> {
            // 最后断点可能只提交了半个文件，不能作为已完成对象过滤。
            if (!key.equals(o.objectKey()) && StringUtils.isNotBlank(eTag)) {
                completedKeys.put(key, eTag);
            }
        }));
        final S3SourceRecordIterator iterator = new S3SourceRecordIterator(sourceConfig,
                TransformerFactory.getTransformer(config.inputFormat()), s3Client,
                (final String key, final String eTag) -> offset
                        .filter(o -> key.equals(o.objectKey()) && StringUtils.isNotBlank(eTag)
                                && eTag.equals(o.processedKeys().get(key)))
                        .map(OffsetStore.Offset::recordCount)
                        .orElse(0L),
                completedKeys, offset.map(OffsetStore.Offset::objectKey).orElse(null));
        return new S3Poller(config, offsetStore, sink, iterator, s3Client, config.s3.bucket);
    }

    /**
     * 执行一轮轮询：从迭代器取记录写入输出层，单轮最多 {@code maxPollRecords} 条。
     * 本轮有产出时先 flush 确认送达，再持久化断点。
     *
     * @return 本轮输出的记录数。
     * @throws IOException
     *             输出层 flush 失败（如 Kafka 发送失败），此时断点未推进。
     */
    public long pollOnce() throws IOException {
        if (stopped.get()) {
            throw new IllegalStateException("轮询器已停止，必须从持久化断点重新创建实例");
        }
        try {
            long produced = 0;
            String lastKey = null;
            String lastETag = null;
            long lastRecordCount = 0;
            final long maxRecords = config.poll.maxPollRecords;
            while ((maxRecords <= 0 || produced < maxRecords) && iterator.hasNext()) {
                final PollerRecord record = iterator.next().toPollerRecord();
                sink.write(record);
                produced++;
                lastKey = record.nativeKey();
                lastETag = record.nativeETag();
                lastRecordCount = record.recordCount();
            }
            if (produced > 0) {
                // 全批确认成功后才保存；失败实例不可继续，重启后重发未提交记录。
                sink.flush();
                offsetStore.save(bucket, lastKey, lastRecordCount, lastETag);
                LOGGER.info("本轮输出 {} 条记录，断点已保存: {} 的第 {} 条，eTag={}", produced, lastKey, lastRecordCount,
                        lastETag);
            }
            return produced;
        } catch (final IOException | RuntimeException e) {
            stop();
            throw e;
        }
    }

    /**
     * 运行轮询循环，直到 {@link #stop()} 被调用或线程被中断。
     * 无数据时按配置等待；处理异常直接向入口传播，禁止越过失败批次。
     */
    public void run() {
        LOGGER.info("轮询器启动: bucket={}, inputFormat={}, output={}, 断点存储={}", config.s3.bucket,
                config.parse.inputFormat, config.output.type, config.offsets.type());
        while (!stopped.get()) {
            try {
                final long produced = pollOnce();
                if (produced == 0 && !stopped.get()) {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(config.poll.intervalSeconds));
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("轮询线程被中断，退出");
                break;
            } catch (final Exception e) {
                stop();
                LOGGER.error("轮询失败，停止推进，重启后从持久化断点恢复: {}", e.getMessage(), e);
                throw new IllegalStateException("轮询失败，未提交记录需要重试", e);
            }
        }
        LOGGER.info("轮询器已退出");
    }

    /** 请求停止：当前循环结束后退出 */
    public void stop() {
        stopped.set(true);
    }

    @Override
    public void close() throws IOException {
        stop();
        try (sink; iterator) {
            // 只清理资源，关闭期间送达的记录不推进断点。
        } finally {
            s3Client.shutdown();
        }
    }

    /**
     * YAML 配置 → 旧 S3SourceConfig 属性 key 的映射。
     *
     * <p>
     * 键名引用各 fragment 的公开常量，保证与旧 ConfigDef 一致；
     * task.id/tasks.max 必须显式设置（旧配置从 originalsStrings 强解析）。
     */
    private static Map<String, String> toSourceProps(final PollerConfig config) {
        final Map<String, String> props = new HashMap<>();
        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, config.s3.accessKeyId);
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, config.s3.secretAccessKey);
        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, config.s3.bucket);
        if (StringUtils.isNotBlank(config.s3.endpoint)) {
            props.put(S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG, config.s3.endpoint);
        }
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, config.s3.region);
        if (StringUtils.isNotBlank(config.s3.prefix)) {
            props.put(S3ConfigFragment.AWS_S3_PREFIX_CONFIG, config.s3.prefix);
        }
        props.put(S3ConfigFragment.FETCH_PAGE_SIZE, String.valueOf(config.s3.fetchPageSize));
        props.put(S3ConfigFragment.AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG,
                String.valueOf(config.s3.retryBackoff.delayMs));
        props.put(S3ConfigFragment.AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG,
                String.valueOf(config.s3.retryBackoff.maxDelayMs));
        props.put(S3ConfigFragment.AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG,
                String.valueOf(config.s3.retryBackoff.maxRetries));
        // FilePatternUtils 要求模板非空，未配置时用 .* 匹配所有对象
        props.put(FileNameFragment.FILE_NAME_TEMPLATE_CONFIG,
                StringUtils.isNotBlank(config.poll.filePattern) ? config.poll.filePattern : MATCH_ALL_PATTERN);
        props.put(FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG, config.compressionType().name());
        props.put(TransformerFragment.INPUT_FORMAT_KEY, config.inputFormat().name());
        props.put(TransformerFragment.TRANSFORMER_MAX_BUFFER_SIZE, String.valueOf(config.parse.maxBufferSize));
        if (StringUtils.isNotBlank(config.poll.nativeStartKey)) {
            props.put(SourceConfigFragment.NATIVE_START_KEY, config.poll.nativeStartKey);
        }
        props.put(SourceConfigFragment.RING_BUFFER_SIZE, String.valueOf(config.poll.ringBufferSize));
        // 单进程轮询：固定单任务 + object_hash 分配（tasks.max=1 时所有对象都归本任务）
        props.put(CommonConfig.TASK_ID, "0");
        props.put(CommonConfig.MAX_TASKS, "1");
        return props;
    }
}
