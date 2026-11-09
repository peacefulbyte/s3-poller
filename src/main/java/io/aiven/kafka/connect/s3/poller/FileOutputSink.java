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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

import io.aiven.kafka.connect.common.source.PollerRecord;
import io.aiven.kafka.connect.common.source.input.InputFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 本地文件输出（测试模式）。
 *
 * <ul>
 * <li>{@code per-object}：每个 S3 对象对应一个本地文件（对象 key 中的 '/' 转义为 '_'），
 * 对象内记录按序写入，对象切换时自动开新文件</li>
 * <li>{@code append}：所有对象追加写入同一文件 {@code s3-poller.out}</li>
 * </ul>
 *
 * <p>
 * 编码与 Kafka 输出保持一致：JSONL 每行一条 JSON（UTF-8）；BYTES/BYTES_LINE 原样字节
 * （bytes-line 每行一条，写文件时补换行分隔）；AVRO/PARQUET 每条记录 = 4 字节大端长度前缀
 * + 裸 Avro 字节（长度前缀标记消息边界，对应 Kafka 单条消息）。
 */
public final class FileOutputSink implements OutputSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileOutputSink.class);

    /** append 模式的固定文件名 */
    private static final String APPEND_FILE_NAME = "s3-poller.out";

    private final Mode mode;
    private final Path directory;
    private final InputFormat inputFormat;
    private final RecordValueEncoder encoder;

    /** 当前输出流；per-object 模式随对象切换，append 模式全程复用 */
    private OutputStream currentOut;
    /** per-object 模式：当前正在写入的对象 key */
    private String currentKey;
    /** 当前输出文件路径（日志用） */
    private Path currentFile;

    /**
     * 构造文件输出。
     *
     * @param config
     *            文件输出配置（directory、mode）。
     * @param inputFormat
     *            输入格式，决定值编码方式。
     */
    public FileOutputSink(final PollerConfig.FileOutput config, final InputFormat inputFormat) {
        this.mode = Mode.parse(config.mode);
        this.directory = Path.of(config.directory);
        this.inputFormat = inputFormat;
        this.encoder = new RecordValueEncoder();
    }

    @Override
    public void write(final PollerRecord record) throws IOException {
        if (mode == Mode.PER_OBJECT) {
            // 对象切换：关闭上一个对象的文件，为新对象创建文件
            if (!record.nativeKey().equals(currentKey)) {
                closeCurrent();
                currentKey = record.nativeKey();
                currentFile = directory.resolve(sanitize(record.nativeKey()));
                Files.createDirectories(directory);
                currentOut = Files.newOutputStream(currentFile, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
            }
        } else if (currentOut == null) {
            Files.createDirectories(directory);
            currentFile = directory.resolve(APPEND_FILE_NAME);
            currentOut = Files.newOutputStream(currentFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        currentOut.write(encodeRecord(record));
        if (inputFormat == InputFormat.JSONL || inputFormat == InputFormat.BYTES_LINE) {
            // JSONL / bytes-line 均为逐行记录，写文件时补换行分隔
            currentOut.write('\n');
        }
    }

    @Override
    public void flush() throws IOException {
        if (currentOut != null) {
            currentOut.flush();
        }
    }

    @Override
    public void close() throws IOException {
        closeCurrent();
    }

    /** 按格式编码一条记录的输出字节 */
    private byte[] encodeRecord(final PollerRecord record) throws IOException {
        final byte[] value = encoder.encodeValue(record.valueData(), inputFormat);
        if (inputFormat == InputFormat.JSONL || inputFormat == InputFormat.BYTES
                || inputFormat == InputFormat.BYTES_LINE) {
            return value;
        }
        // AVRO/PARQUET：4 字节大端长度前缀，标记每条裸 Avro 消息的边界
        final ByteBuffer buffer = ByteBuffer.allocate(4 + value.length);
        buffer.putInt(value.length);
        buffer.put(value);
        return buffer.array();
    }

    private void closeCurrent() throws IOException {
        if (currentOut != null) {
            currentOut.flush();
            currentOut.close();
            LOGGER.info("已写出文件: {}", currentFile);
            currentOut = null;
        }
    }

    /**
     * 对象 key 转本地文件名：'/' 与 '\' 转义为 '_'，避免目录穿越与路径歧义。
     *
     * @param objectKey
     *            S3 对象 key。
     * @return 可直接用作文件名的字符串。
     */
    static String sanitize(final String objectKey) {
        return objectKey.replace('/', '_').replace('\\', '_');
    }

    /** 文件输出模式 */
    public enum Mode {
        /** 每个 S3 对象一个本地文件 */
        PER_OBJECT,
        /** 全部追加到单一文件 */
        APPEND;

        public static Mode parse(final String name) {
            try {
                return valueOf(name.toUpperCase(Locale.ROOT).replace('-', '_'));
            } catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("不支持的 output.file.mode: " + name + "，可选 per-object/append");
            }
        }
    }
}
