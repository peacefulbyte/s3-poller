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

import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.commons.collections.RingBuffer;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.source.input.ParquetTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.common.source.task.DistributionStrategy;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;

/**
 * Iterator that processes cloud storage items and creates Kafka source records. Supports multiple output formats.
 *
 * @param <N>
 *            the native object type.
 * @param <K>
 *            the key type for the native object.
 * @param <T>
 *            The source record for the client type.
 *
 */
public abstract class AbstractSourceRecordIterator<K extends Comparable<K>, N, T extends AbstractSourceRecord<K, N, T>>
        implements
            Iterator<T> {
    /**
     * Function that returns the number of records already extracted for a native item. Used to resume reading an
     * object from the middle after a restart; returns 0 when the object has not been processed before.
     */
    private final Function<K, Long> initialRecordCounts;

    /** The configuration for the source */
    private final SourceCommonConfig sourceConfig;
    /** The transformer for the data conversions */
    private final Transformer transformer;
    /** the taskId of this running task */
    private final int taskId;

    /**
     * The inner iterator to provides a base AbstractSourceRecord for a storage item that has passed the filters and
     * potentially had data extracted.
     */
    private Iterator<T> inner;
    /**
     * The outer iterator that provides an AbstractSourceRecord for each record contained by the storage item identified
     * by the inner record.
     */
    private Iterator<T> outer;

    /** Check if the native item key is part of the 'target' files configured to be extracted from Azure */
    private final FileMatching fileMatching;
    /** The predicate which will determine if an native item should be assigned to this task for processing */
    protected final Predicate<Optional<T>> taskAssignment;
    /**
     * The native item key which is currently being processed, when rehydrating from the storage engine we will skip
     * other native items that come before this key.
     */
    private K lastSeenNativeKey;
    /** 当前正在处理的对象的 eTag，对象消费完毕后与 key 一起记入 eTag 表 */
    private String lastSeenNativeETag;
    /**
     * The ring buffer which contains recently processed native item keys, this is used during a restart to skip keys
     * that are known to have been processed while still accounting for the possibility that slower writing to storage
     * may have introduced newer keys.
     */
    private final RingBuffer<K> ringBuffer;

    /**
     * 已处理对象的 key→eTag 映射，与环形缓冲中的 key 一一对应（eTag 可能为 null）。
     * 扫描时用 key + eTag 联合判断：key 在缓冲中但 eTag 不同（文件被覆盖）时重新消费。
     */
    private final Map<K, String> processedETags = new ConcurrentHashMap<>();

    private final K nativeStartKey;

    /**
     * Constructor.
     *
     * @param sourceConfig
     *            The source configuration.
     * @param transformer
     *            the transformer to use.
     * @param ringBufferSize
     *            the size of the ring buffer.
     * @param initialRecordCounts
     *            function to query the number of records already extracted for a native item.
     * @param initialProcessedKeys
     *            已处理的对象 key→eTag 映射（如从断点文件加载），构造时预填环形缓冲与 eTag 表以跳过重复消费；
     *            值为 null 表示旧断点无 eTag 信息，仅预填环形缓冲。
     */
    public AbstractSourceRecordIterator(final SourceCommonConfig sourceConfig, final Transformer transformer,
            final int ringBufferSize, final Function<K, Long> initialRecordCounts,
            final Map<K, String> initialProcessedKeys) {
        super();

        final DistributionType distributionType = sourceConfig.getDistributionType();
        final int maxTasks = sourceConfig.getMaxTasks();

        this.sourceConfig = sourceConfig;
        this.initialRecordCounts = initialRecordCounts;
        this.transformer = transformer;
        this.taskId = sourceConfig.getTaskId() % maxTasks;
        this.taskAssignment = new TaskAssignment(distributionType.getDistributionStrategy(maxTasks));
        this.nativeStartKey = sourceConfig.getNativeStartKey() != null
                ? parseNativeKey(sourceConfig.getNativeStartKey())
                : null;
        this.fileMatching = new FileMatching(new FilePatternUtils(sourceConfig.getSourceName()));
        this.inner = Collections.emptyIterator();
        this.outer = Collections.emptyIterator();
        this.ringBuffer = new RingBuffer<>(Math.max(1, ringBufferSize));
        // 预填已处理 key：重启后扫描时直接跳过，避免重复消费；有 eTag 的同步存入 eTag 表
        if (initialProcessedKeys != null) {
            initialProcessedKeys.forEach((key, eTag) -> {
                ringBuffer.add(key);
                if (eTag != null) {
                    processedETags.put(key, eTag);
                }
            });
        }
    }

    /**
     * Gets the logger for the concrete implementation.
     *
     * @return The logger for the concrete implementation.
     */
    abstract protected Logger getLogger();

    /**
     * Get a stream of Native object from the underlying storage layer.
     *
     * @param offset
     *            the native key to start from. May be {@code null}.
     * @return A stream of natvie objects. May be empty but not {@code null}.
     */
    abstract protected Stream<N> getNativeItemStream(K offset);

    /**
     *
     * @param nativeKeyText
     *            The native identifier to begin consuming events from
     * @return A transformed native Key into the correct type for processing the specific native key implementation
     */
    protected abstract K parseNativeKey(String nativeKeyText);

    /**
     * Gets an IOSupplier for the specific source record.
     *
     * @param sourceRecord
     *            the source record to get the input stream from.
     * @return the IOSupplier that retrieves an InputStream from the source record.
     */
    abstract protected IOSupplier<InputStream> getInputStream(T sourceRecord);

    /**
     * Gets the native key for the native object.
     *
     * @param nativeObject
     *            the native object ot retrieve the native key for.
     * @return The native key for the native object.
     */
    abstract protected K getNativeKey(N nativeObject);

    /**
     * Gets the ETag for the native object. Used to detect when a file has been overwritten.
     *
     * @param nativeObject
     *            the native object to retrieve the ETag for.
     * @return The ETag of the native object. May be {@code null}.
     */
    abstract protected String getNativeETag(N nativeObject);

    /**
     * Gets the AbstractSourceRecord implementation for the native object.
     *
     * @param nativeObject
     *            the native object to get the AbstractSourceRecord for.
     * @return the AbstractSourceRecord for the natvie object.
     */
    abstract protected T createSourceRecord(N nativeObject);

    @Override
    final public boolean hasNext() {
        final boolean outerHasNext;
        try {
            outerHasNext = outer.hasNext();
        } catch (final RuntimeException e) {
            // 对象下载/解析失败（如 IfMatch 412、网络抖动）：丢弃当前对象状态（不记入去重缓冲），
            // 向上抛出让轮询器本轮失败；下轮重新扫描会再次遇到该对象并重新下载，避免数据丢失
            outer = Collections.emptyIterator();
            lastSeenNativeKey = null;
            lastSeenNativeETag = null;
            throw e;
        }
        if (!outerHasNext && lastSeenNativeKey != null) {
            // 上一个对象的记录已全部产出：key 与 eTag 同时记入去重缓冲/表，并清理被挤出 key 的 eTag；
            // eTag 必须在此刻（而非放行时）更新，否则消费失败会导致该 key 在下轮扫描被永久跳过；
            // 断点持久化由轮询器拿到记录后完成
            // update the buffer to contain this new objectKey
            final K ejected = ringBuffer.add(lastSeenNativeKey);
            if (ejected != null) {
                processedETags.remove(ejected);
            }
            if (lastSeenNativeETag != null) {
                processedETags.put(lastSeenNativeKey, lastSeenNativeETag);
            }
        }
        if (!inner.hasNext() && !outer.hasNext()) {
            // 当前批次已读完，重新扫描存储桶；起点优先取环形缓冲中最后处理过的 key
            inner = getNativeItemStream(ObjectUtils.getIfNull(ringBuffer.getNextEjected(), () -> {
                getLogger().info("{} set, no alternative present in buffer will begin consuming from {}",
                        SourceConfigFragment.NATIVE_START_KEY, nativeStartKey);
                return nativeStartKey;
            })).map(fileMatching).filter(taskAssignment).filter(Optional::isPresent).map(Optional::get).iterator();
        }
        while (!outer.hasNext() && inner.hasNext()) {
            outer = convert(inner.next()).iterator();
        }
        return outer.hasNext();
    }

    @Override
    final public T next() {
        return outer.next();
    }

    @Override
    final public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

    /**
     * Converts the native item into stream of AbstractSourceRecords.
     *
     * @param sourceRecord
     *            the SourceRecord that drives the creation of source records with values.
     * @return a stream of T created from the input stream of the native item.
     */
    @VisibleForTesting
    Stream<T> convert(final T sourceRecord) {
        // 第三步：解析。先生成记录 key 并记下当前对象及其 eTag，供下一轮扫描定位起点与 eTag 去重
        sourceRecord
                .setKeyData(transformer.getKeyData(sourceRecord.getNativeKey(), sourceRecord.getTopic(), sourceConfig));

        lastSeenNativeKey = sourceRecord.getNativeKey();
        lastSeenNativeETag = sourceRecord.getNativeETag();

        // 先按 file.compression.type 解压（Parquet 自带压缩，跳过外层解压），再交给 Transformer 逐条解析
        // parquet handles compression internally.
        final CompressionType compressionType = transformer instanceof ParquetTransformer
                ? CompressionType.NONE
                : sourceConfig.getCompressionType();
        // create an IOSupplier with the specified compression;
        // NONE 模式下自动探测对象流 magic bytes（gzip/zstd），命中才解压，否则按非压缩读取
        final IOSupplier<InputStream> inputStream = transformer instanceof ParquetTransformer
                ? getInputStream(sourceRecord)
                : compressionType == CompressionType.NONE
                        ? CompressionType.autoDecompress(getInputStream(sourceRecord))
                        : compressionType.decompress(getInputStream(sourceRecord));
        return transformer
                .getRecords(inputStream, sourceRecord.getNativeItemSize(), sourceRecord.getContext(), sourceConfig,
                        sourceRecord.getRecordCount())
                .map(new Mapper<>(sourceRecord));
    }

    /**
     * Maps the data from the @{link Transformer} stream to an AbstractSourceRecord given all the additional data
     * required.
     *
     * @param <N>
     *            the native object type.
     * @param <K>
     *            the key type for the native object.
     * @param <T>
     *            The source record for the client type.
     */
    static class Mapper<N, K extends Comparable<K>, T extends AbstractSourceRecord<K, N, T>>
            implements
                Function<SchemaAndValue, T> {
        /**
         * The AbstractSourceRecord that produces the values.
         */
        private final T sourceRecord;

        /**
         * Constructor.
         *
         * @param sourceRecord
         *            The source record to provide default values..
         */
        public Mapper(final T sourceRecord) {
            // operation within the Transformer
            // to see if there are more records.
            this.sourceRecord = sourceRecord;
        }

        @Override
        public T apply(final SchemaAndValue valueData) {
            sourceRecord.incrementRecordCount();
            final T result = sourceRecord.duplicate();
            result.setValueData(valueData);
            return result;
        }
    }

    /**
     * Determines if an AbstractSourceRecord belongs to this task.
     */
    class TaskAssignment implements Predicate<Optional<T>> {
        private final DistributionStrategy distributionStrategy;
        /**
         * Constructs a task assignment from the distribution strategy.
         *
         * @param distributionStrategy
         *            The distribution strategy.
         */
        TaskAssignment(final DistributionStrategy distributionStrategy) {
            this.distributionStrategy = distributionStrategy;
        }

        @Override
        public boolean test(final Optional<T> sourceRecord) {
            return sourceRecord.filter(t -> taskId == distributionStrategy.getTaskFor(t.getContext())).isPresent();
        }
    }

    /**
     * Attempts to match the name of the native item and extract the Context from it.
     */
    class FileMatching implements Function<N, Optional<T>> {
        /**
         * The file pattern utils that this file matching uses.
         */
        private final FilePatternUtils utils;

        /**
         * Created a FileMatching from the specified FilePatternUtils.
         *
         * @param utils
         *            the file pattern utils to use.
         */
        FileMatching(final FilePatternUtils utils) {
            this.utils = utils;
        }

        @Override
        public Optional<T> apply(final N nativeItem) {
            final K itemName = getNativeKey(nativeItem);
            final Optional<Context<K>> optionalContext = utils.process(itemName);
            if (!optionalContext.isPresent()) {
                return Optional.empty();
            }
            // 去重判断：key 不在环形缓冲中，或已记录 eTag 与当前 eTag 不同（文件被覆盖）→ 重新消费
            final boolean processed = ringBuffer.contains(itemName);
            final String recordedETag = processedETags.get(itemName);
            final boolean overwritten = recordedETag != null && !recordedETag.equals(getNativeETag(nativeItem));
            if (processed && !overwritten) {
                return Optional.empty();
            }
            final T sourceRecord = createSourceRecord(nativeItem);
            final Context<K> context = optionalContext.get();
            overrideContextTopic(context);
            sourceRecord.setContext(context);
            // 断点续读：查询该对象已处理的记录数，解析时跳过；文件被覆盖时从头消费，旧 recordCount 失效
            sourceRecord.setRecordCount(overwritten ? 0L : initialRecordCounts.apply(itemName));
            return Optional.of(sourceRecord);
        }

        /**
         * Sets the target topic in the context.
         *
         * @param context
         *            the context to set the topic in if found.
         */
        private void overrideContextTopic(final Context<K> context) {
            final String targetTopic = sourceConfig.getTargetTopic();
            if (targetTopic != null) {
                if (context.getTopic().isPresent()) {
                    getLogger().debug(
                            "Overriding topic '{}' extracted from native item name with topic '{}' from configuration 'topics'. ",
                            context.getTopic().get(), targetTopic);
                }
                context.setTopic(targetTopic);
            }
        }
    }
}
