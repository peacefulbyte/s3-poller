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
 * <li>断点文件同时维护最近已消费对象的 key→eTag 映射，重启后预填迭代器的去重缓冲
 * 与 eTag 表：key 相同但 eTag 不同（文件被覆盖）的对象会重新消费</li>
 * <li>单轮输出条数受 {@code poll.maxPollRecords} 限制，防止持续有新对象时
 * 断点长时间得不到持久化</li>
 * </ul>
 */
public final class S3Poller implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3Poller.class);

    /** filePattern 未配置时的兜底模板：FilePatternUtils 要求非空，正则 .* 匹配所有对象 */
    static final String MATCH_ALL_PATTERN = ".*";

    private final PollerConfig config;
    private final FileOffsetStore offsetStore;
    private final OutputSink sink;
    private final S3SourceRecordIterator iterator;
    private final AWSV2SourceClient s3Client;
    private final String bucket;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private S3Poller(final PollerConfig config, final FileOffsetStore offsetStore, final OutputSink sink,
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
     * 不会发起网络请求（S3 客户端与扫描都是懒加载）。
     *
     * @param config
     *            已校验的 YAML 配置。
     * @return 就绪的轮询器。
     */
    public static S3Poller create(final PollerConfig config) {
        final S3SourceConfig s3SourceConfig = new S3SourceConfig(toSourceProps(config));
        // 临时文件下载目录：为空时交给 AWSV2SourceClient 使用系统临时目录
        final Path downloadDir = StringUtils.isNotBlank(config.poll.downloadDir)
                ? Path.of(config.poll.downloadDir)
                : null;
        final AWSV2SourceClient s3Client = new AWSV2SourceClient(s3SourceConfig, downloadDir);
        final FileOffsetStore offsetStore = new FileOffsetStore(Path.of(config.offsets.file),
                config.poll.ringBufferSize);
        final Optional<FileOffsetStore.Offset> offset = offsetStore.load();
        // 重启后：断点对象从 recordCount 处续读（兜底，对象被覆盖时迭代器会忽略该续读从头消费），
        // 已消费对象 key→eTag 映射预填去重缓冲与 eTag 表
        final Map<String, String> processedKeys = offset.map(FileOffsetStore.Offset::processedKeys).orElse(Map.of());
        final S3SourceRecordIterator iterator = new S3SourceRecordIterator(s3SourceConfig,
                TransformerFactory.getTransformer(config.inputFormat()), s3Client,
                key -> offset
                        .filter(o -> config.s3.bucket.equals(o.bucket()) && key.equals(o.objectKey()))
                        .map(FileOffsetStore.Offset::recordCount)
                        .orElse(0L),
                processedKeys);
        final OutputSink sink = config.output.type() == PollerConfig.OutputType.FILE
                ? new FileOutputSink(config.output.file, config.inputFormat())
                : new KafkaOutputSink(config.output.kafka, config.inputFormat());
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
        long produced = 0;
        String lastKey = null;
        String lastETag = null;
        long lastRecordCount = 0;
        final long maxRecords = config.poll.maxPollRecords;
        while (iterator.hasNext() && (maxRecords <= 0 || produced < maxRecords)) {
            final PollerRecord record = iterator.next().toPollerRecord();
            sink.write(record);
            produced++;
            lastKey = record.nativeKey();
            lastETag = record.nativeETag();
            lastRecordCount = record.recordCount();
        }
        if (produced > 0) {
            // 先确认送达再推进断点：flush 抛错时断点不动，重启后重发本批（at-least-once）
            sink.flush();
            offsetStore.save(bucket, lastKey, lastRecordCount, lastETag);
            LOGGER.info("本轮输出 {} 条记录，断点已保存: {} 的第 {} 条，eTag={}", produced, lastKey, lastRecordCount,
                    lastETag);
        }
        return produced;
    }

    /**
     * 运行轮询循环，直到 {@link #stop()} 被调用或线程被中断。
     * 无数据或发生异常时休眠 {@code poll.intervalSeconds} 秒后继续。
     */
    public void run() {
        LOGGER.info("轮询器启动: bucket={}, inputFormat={}, output={}, 断点文件={}", config.s3.bucket,
                config.parse.inputFormat, config.output.type, config.offsets.file);
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
                LOGGER.error("轮询异常，{} 秒后继续: {}", config.poll.intervalSeconds, e.getMessage(), e);
                if (sleepQuietly(TimeUnit.SECONDS.toMillis(config.poll.intervalSeconds))) {
                    break;
                }
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
        sink.close();
        s3Client.shutdown();
    }

    /** 休眠并处理中断；返回 true 表示已被中断，调用方应退出 */
    private boolean sleepQuietly(final long millis) {
        try {
            Thread.sleep(millis);
            return false;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
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
