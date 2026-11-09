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

package io.aiven.kafka.connect.s3.source.utils;

import java.io.InputStream;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class S3SourceRecordIterator
        extends
            AbstractSourceRecordIterator<String, S3Object, S3SourceRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceRecordIterator.class);

    /** The AWS client that provides the S3Objects */
    private final AWSV2SourceClient sourceClient;

    /**
     * 构造 S3 迭代器。
     *
     * @param s3SourceConfig
     *            S3 源配置。
     * @param transformer
     *            数据转换器。
     * @param sourceClient
     *            S3 客户端。
     * @param initialRecordCounts
     *            查询对象已处理记录数的函数，用于断点续读。
     * @param initialProcessedKeys
     *            已处理的对象 key 列表，预填去重缓冲。
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "source client has stores mutable fields")
    public S3SourceRecordIterator(final S3SourceConfig s3SourceConfig, final Transformer transformer,
            final AWSV2SourceClient sourceClient, final Function<String, Long> initialRecordCounts,
            final Collection<String> initialProcessedKeys) {

        super(s3SourceConfig, transformer, s3SourceConfig.getRingBufferSize(), initialRecordCounts,
                initialProcessedKeys);
        this.sourceClient = sourceClient;
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @Override
    protected Stream<S3Object> getNativeItemStream(final String offset) {
        // 第一步：扫描存储桶，得到本次待处理的对象流
        return sourceClient.getS3ObjectStream(offset);
    }

    @Override
    protected IOSupplier<InputStream> getInputStream(final S3SourceRecord sourceRecord) {
        // 第二步：按对象 key 下载文件内容
        return sourceClient.getObject(sourceRecord.getNativeKey());
    }

    @Override
    protected String getNativeKey(final S3Object nativeObject) {
        return nativeObject.key();
    }

    @Override
    protected String parseNativeKey(final String nativeKeyText) {
        return nativeKeyText;
    }

    @Override
    protected S3SourceRecord createSourceRecord(final S3Object nativeObject) {
        return new S3SourceRecord(nativeObject);
    }
}
