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
import java.util.Locale;
import java.util.Objects;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.source.input.InputFormat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * 轮询器全局配置，对应一份 poller.yaml，共五块：
 *
 * <ul>
 * <li>{@code s3}：S3 兼容存储连接（endpoint、AK/SK、bucket、分页大小、重试）</li>
 * <li>{@code poll}：轮询行为（间隔、文件名过滤、外层压缩、起始 key）</li>
 * <li>{@code parse}：解析参数（输入格式、bytes 切块大小）</li>
 * <li>{@code output}：输出方式（file 测试 / kafka 生产）</li>
 * <li>{@code offsets}：断点文件路径</li>
 * </ul>
 *
 * <p>
 * 字段全部公开，由 jackson 直接映射；YAML 未出现的字段使用默认值。
 * 加载入口 {@link #load(Path)}，加载后立即执行 {@link #validate()} 校验。
 */
public class PollerConfig {

    public S3 s3 = new S3();
    public Poll poll = new Poll();
    public Parse parse = new Parse();
    public Output output = new Output();
    public Offsets offsets = new Offsets();

    /**
     * 从 YAML/JSON 文件加载配置并校验。
     *
     * @param configFile
     *            配置文件路径。
     * @return 校验通过的配置。
     * @throws IOException
     *             文件读取或格式错误。
     * @throws IllegalArgumentException
     *             必填项缺失或枚举取值不合法。
     */
    public static PollerConfig load(final Path configFile) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final PollerConfig config = mapper.readValue(configFile.toFile(), PollerConfig.class);
        config.validate();
        return config;
    }

    /**
     * 校验必填项与枚举取值，不合法时抛出 IllegalArgumentException。
     */
    public void validate() {
        Objects.requireNonNull(s3.bucket, "s3.bucket 不能为空");
        Objects.requireNonNull(s3.accessKeyId, "s3.accessKeyId 不能为空");
        Objects.requireNonNull(s3.secretAccessKey, "s3.secretAccessKey 不能为空");
        inputFormat(); // 校验并提前抛错
        compressionType(); // 校验并提前抛错
        if (output.type() == OutputType.KAFKA) {
            Objects.requireNonNull(output.kafka.bootstrapServers, "output.kafka.bootstrapServers 不能为空");
            if (output.kafka.topic == null && poll.filePattern == null) {
                throw new IllegalArgumentException("output.kafka.topic 与 poll.filePattern 至少配置一个，用于确定目标 topic");
            }
        } else {
            Objects.requireNonNull(output.file.directory, "output.file.directory 不能为空");
        }
        if (offsets.type().equalsIgnoreCase("mysql")) {
            Objects.requireNonNull(offsets.mysql.jdbcUrl, "offsets.mysql.jdbcUrl 不能为空");
            Objects.requireNonNull(offsets.mysql.username, "offsets.mysql.username 不能为空");
        }
    }

    /**
     * 解析格式（parse.inputFormat）。
     *
     * @return 对应的 InputFormat 枚举。
     */
    public InputFormat inputFormat() {
        try {
            return InputFormat.valueOf(parse.inputFormat.toUpperCase(Locale.ROOT).replace('-', '_'));
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "不支持的 inputFormat: " + parse.inputFormat + "，可选 avro/parquet/jsonl/bytes/bytes-line");
        }
    }

    /**
     * 对象外层压缩类型（poll.compressionType）。
     *
     * @return 对应的 CompressionType 枚举。
     */
    public CompressionType compressionType() {
        try {
            return CompressionType.forName(poll.compressionType);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("不支持的 compressionType: " + poll.compressionType + "，可选 none/gzip/snappy/zstd");
        }
    }

    /** S3 兼容存储连接配置 */
    public static class S3 {
        /** 存储服务地址，如 http://127.0.0.1:9000；为空则使用官方 AWS */
        public String endpoint;
        public String region = "us-east-1";
        /**
         * 是否使用 path-style 访问（桶名在 URL 路径中）。
         * 默认 false=virtual-hosted-style（桶名作为子域名），匹配 AWS / 腾讯云 COS；
         * MinIO 等本地测试服务无 DNS 解析时设为 true。
         */
        public boolean pathStyleAccess = false;
        /** 存储桶名，必填 */
        public String bucket;
        /** 只扫描该前缀下的对象，可为空 */
        public String prefix;
        public String accessKeyId;
        public String secretAccessKey;
        /** 每次 ListObjects 的分页大小 */
        public int fetchPageSize = 100;
        /** 请求重试参数 */
        public RetryBackoff retryBackoff = new RetryBackoff();
    }

    /** S3 请求重试参数，对应原有 aws.s3.backoff.* 配置 */
    public static class RetryBackoff {
        public long delayMs = 100;
        public long maxDelayMs = 20000;
        public int maxRetries = 10;
    }

    /** 轮询行为配置 */
    public static class Poll {
        /** 一轮扫描完后的休眠秒数；有积压时可配置 0 连续扫描 */
        public long intervalSeconds = 10;
        /** 文件名过滤模板，如 {{topic}}-{{partition}}-{{start_offset}}；为空则匹配所有对象 */
        public String filePattern;
        /** 对象外层压缩：none / gzip / snappy / zstd */
        public String compressionType = "none";
        /** 从该 key 开始消费；为空则从断点或桶首开始 */
        public String nativeStartKey;
        /** 已消费对象 key 的内存去重缓冲大小，重启后由断点文件预填 */
        public int ringBufferSize = 1000;
        /** 单轮轮询最多输出的记录数，达到后先持久化断点再继续；0 表示不限制 */
        public int maxPollRecords = 500;
        /** 对象下载临时目录；为空使用系统临时目录。配置专用目录时启动会清理其中 s3-poller-*.obj 残留 */
        public String downloadDir;
    }

    /** 解析配置 */
    public static class Parse {
        /** 输入格式：avro / parquet / jsonl / bytes / bytes-line */
        public String inputFormat = "jsonl";
        /** bytes 格式的切块大小；bytes-line 格式的单行最大字节数（超长行跳过并告警） */
        public int maxBufferSize = 4096;
    }

    /** 输出配置：file（测试用）或 kafka（生产用） */
    public static class Output {
        /** file | kafka */
        public String type = "file";
        public FileOutput file = new FileOutput();
        public KafkaOutput kafka = new KafkaOutput();

        public OutputType type() {
            try {
                return OutputType.valueOf(type.toUpperCase(Locale.ROOT));
            } catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("不支持的 output.type: " + type + "，可选 file/kafka");
            }
        }
    }

    /** 输出到本地文件的参数（测试模式） */
    public static class FileOutput {
        /** 输出目录 */
        public String directory = "./out";
        /** per-object=每个 S3 对象一个文件；append=全部追加到单一文件 */
        public String mode = "per-object";
    }

    /** 输出到 Kafka 的参数（生产模式） */
    public static class KafkaOutput {
        public String bootstrapServers;
        /** 固定目标 topic；为空则使用 filePattern 中提取的 topic */
        public String topic;
        public String acks = "all";
        public Security security = new Security();
    }

    /** Kafka 连接安全配置 */
    public static class Security {
        /** PLAINTEXT 或 SASL_SSL */
        public String protocol = "PLAINTEXT";
        public String username;
        public String password;
    }

    /** 断点持久化配置 */
    public static class Offsets {
        /** 断点存储类型：file=本地 JSON 文件 / mysql=MySQL 表 */
        public String type = "file";
        /** 断点文件路径；type=file 时使用，进程重启后从该文件记录的 key 之后继续扫描 */
        public String file = "./offsets.json";
        /** type=mysql 时的连接配置 */
        public MySql mysql = new MySql();

        public String type() {
            return type == null ? "file" : type.trim();
        }
    }

    /** MySQL 断点存储连接配置 */
    public static class MySql {
        /** JDBC 连接串，如 jdbc:mysql://127.0.0.1:3306/s3_poller?useSSL=false */
        public String jdbcUrl;
        public String username;
        public String password;
        /** 断点表名，只允许字母/数字/下划线，默认 s3_offset */
        public String table = "s3_offset";
        /** 已消费对象窗口大小；为空则取 poll.ringBufferSize */
        public Integer maxProcessedKeys;
    }

    /** 输出方式枚举 */
    public enum OutputType {
        FILE,
        KAFKA
    }
}
