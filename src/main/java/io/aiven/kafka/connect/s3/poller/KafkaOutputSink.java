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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import io.aiven.kafka.connect.common.source.PollerRecord;
import io.aiven.kafka.connect.common.source.input.InputFormat;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka 输出（生产模式）。
 *
 * <p>
 * 直接把解析结果发送到 Kafka：key 为对象 key 的 UTF-8 字节，value 按输入格式编码
 * （JSONL 一条 JSON / BYTES·BYTES_LINE 原样字节 / AVRO·PARQUET 裸 Avro 字节，见 {@link RecordValueEncoder}）。
 * 不接 Schema Registry，消费端需已知 schema（bytes-line 场景由消费端按实际编码解码）。
 *
 * <p>
 * 发送语义（at-least-once）：{@link #write} 异步发送；每批结束调 {@link #flush}，
 * 若本批存在重试耗尽仍失败的记录则抛出 IOException——轮询器捕获后不推进断点，
 * 重启后整批重发。
 */
public final class KafkaOutputSink implements OutputSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaOutputSink.class);

    private final KafkaProducer<byte[], byte[]> producer;
    /** 配置的固定 topic；为空时使用记录携带的 topic */
    private final String topic;
    private final InputFormat inputFormat;
    private final RecordValueEncoder encoder;
    /** 本批次发送失败（重试耗尽）的记录数 */
    private final AtomicLong failedSends = new AtomicLong();

    /**
     * 构造 Kafka 输出。
     *
     * @param config
     *            Kafka 输出配置（bootstrapServers、topic、acks、security）。
     * @param inputFormat
     *            输入格式，决定 value 编码方式。
     */
    public KafkaOutputSink(final PollerConfig.KafkaOutput config, final InputFormat inputFormat) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, config.acks);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        if ("SASL_SSL".equalsIgnoreCase(config.security.protocol)) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put(SaslConfigs.SASL_JAAS_CONFIG, plainJaas(config.security.username, config.security.password));
        }
        // PLAINTEXT 使用客户端默认值，无需额外配置
        this.producer = new KafkaProducer<>(props);
        this.topic = config.topic;
        this.inputFormat = inputFormat;
        this.encoder = new RecordValueEncoder();
    }

    @Override
    public void write(final PollerRecord record) throws IOException {
        final String targetTopic = topic != null ? topic : record.topic();
        if (targetTopic == null) {
            throw new IllegalStateException("记录未携带 topic 且未配置 output.kafka.topic: " + record.nativeKey());
        }
        final byte[] key = (byte[]) record.keyData().value();
        final byte[] value = encoder.encodeValue(record.valueData(), inputFormat);
        producer.send(new ProducerRecord<>(targetTopic, record.partition(), key, value), (metadata, exception) -> {
            if (exception != null) {
                failedSends.incrementAndGet();
                LOGGER.warn("Kafka 发送失败（重试已耗尽） key={} recordCount={}: {}", record.nativeKey(),
                        record.recordCount(), exception.getMessage());
            }
        });
    }

    @Override
    public void flush() throws IOException {
        producer.flush();
        final long failed = failedSends.getAndSet(0);
        if (failed > 0) {
            throw new IOException("本批次有 " + failed + " 条记录发送失败，断点不推进，重启后整批重发");
        }
    }

    @Override
    public void close() throws IOException {
        producer.flush();
        producer.close();
    }

    /**
     * 生成 SASL PLAIN 机制的 JAAS 配置字符串。
     *
     * @param username
     *            用户名。
     * @param password
     *            密码。
     * @return JAAS 配置，用户名密码中的引号与反斜杠已转义。
     */
    static String plainJaas(final String username, final String password) {
        return String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                escape(username), escape(password));
    }

    private static String escape(final String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
