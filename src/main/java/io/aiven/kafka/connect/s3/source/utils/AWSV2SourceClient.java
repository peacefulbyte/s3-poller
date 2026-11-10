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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Called AWSV2SourceClient as this source client implements the V2 version of the aws client library. Handles all calls
 * and authentication to AWS and returns useable objects to the AbstractSourceRecordIterator.
 */
public class AWSV2SourceClient {

    private final S3SourceConfig s3SourceConfig;
    private final S3Client s3Client;
    private final String bucketName;
    /** 对象下载临时目录；null 时使用系统临时目录 */
    private final Path downloadDir;

    private Predicate<S3Object> filterPredicate = s3Object -> s3Object.size() > 0;

    /**
     * @param s3SourceConfig
     *            configuration for Source connector
     */
    public AWSV2SourceClient(final S3SourceConfig s3SourceConfig) {
        this(s3SourceConfig, null);
    }

    /**
     * @param s3SourceConfig
     *            configuration for Source connector
     * @param pathStyleAccess
     *            true=桶名在 URL 路径中（MinIO 等本地服务）；false=桶名作为子域名（AWS / 腾讯云 COS）。
     */
    public AWSV2SourceClient(final S3SourceConfig s3SourceConfig, final boolean pathStyleAccess) {
        this(new S3ClientFactory().createAmazonS3Client(s3SourceConfig, pathStyleAccess), s3SourceConfig, null);
    }

    /**
     * @param s3SourceConfig
     *            configuration for Source connector
     * @param downloadDir
     *            对象下载临时目录，null 时使用系统临时目录
     */
    public AWSV2SourceClient(final S3SourceConfig s3SourceConfig, final Path downloadDir) {
        this(s3SourceConfig, downloadDir, false);
    }

    /**
     * @param s3SourceConfig
     *            configuration for Source connector
     * @param downloadDir
     *            对象下载临时目录，null 时使用系统临时目录
     * @param pathStyleAccess
     *            true=桶名在 URL 路径中（MinIO 等本地服务）；false=桶名作为子域名（AWS / 腾讯云 COS）。
     */
    public AWSV2SourceClient(final S3SourceConfig s3SourceConfig, final Path downloadDir,
            final boolean pathStyleAccess) {
        this(new S3ClientFactory().createAmazonS3Client(s3SourceConfig, pathStyleAccess), s3SourceConfig, downloadDir);
    }

    /**
     * Valid for testing
     *
     * @param s3Client
     *            amazonS3Client
     * @param s3SourceConfig
     *            configuration for Source connector
     */
    AWSV2SourceClient(final S3Client s3Client, final S3SourceConfig s3SourceConfig) {
        this(s3Client, s3SourceConfig, null);
    }

    /**
     * Valid for testing
     *
     * @param s3Client
     *            amazonS3Client
     * @param s3SourceConfig
     *            configuration for Source connector
     * @param downloadDir
     *            对象下载临时目录，null 时使用系统临时目录
     */
    AWSV2SourceClient(final S3Client s3Client, final S3SourceConfig s3SourceConfig, final Path downloadDir) {
        this.s3SourceConfig = s3SourceConfig;
        this.s3Client = s3Client;
        this.bucketName = s3SourceConfig.getAwsS3BucketName();
        this.downloadDir = downloadDir;
    }

    /**
     * Creates a stream from which we will create an iterator.
     *
     * @param startToken
     *            the beginning key, or {@code null} to start at the beginning.
     * @return a Stream of S3Objects for the current state of the S3 storage.
     */
    public Stream<S3Object> getS3ObjectStream(final String startToken) {
        // 扫描存储桶：从 startToken 之后开始，按 fetch.page.size 分页拉取对象元数据（懒加载）
        final ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(s3SourceConfig.getFetchPageSize())
                .prefix(s3SourceConfig.getAwsS3Prefix())
                .startAfter(StringUtils.defaultIfBlank(startToken, null))
                .build();

        return Stream.iterate(s3Client.listObjectsV2(request), Objects::nonNull, response -> {
            // This is called every time next() is called on the iterator.
            // 未读完则用 continuationToken 取下一页，读完返回 null 结束整个流
            if (response.isTruncated()) {
                return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .maxKeys(s3SourceConfig.getFetchPageSize())
                        .continuationToken(response.nextContinuationToken())
                        .build());
            } else {
                return null;
            }

        }).flatMap(response -> response.contents().stream().filter(filterPredicate));
    }

    /**
     * Gets an iterator of keys from the current S3 storage.
     *
     * @param startToken
     *            the beginning key, or {@code null} to start at the beginning.
     * @return an Iterator on the keys of the current S3Objects.
     */
    public Iterator<String> getListOfObjectKeys(final String startToken) {
        return getS3ObjectStream(startToken).map(S3Object::key).iterator();
    }

    /**
     * 下载对象：流式下载到本地临时文件，返回惰性的文件流交给解析器逐条消费；
     * 内存占用 O(缓冲区)，大对象不再整块进入 JVM 堆。
     *
     * @param objectKey
     *            对象 key。
     * @param expectedETag
     *            扫描（listObjects）时拿到的 eTag，作为 IfMatch 前置条件；为 {@code null} 时不校验。
     *            扫描到下载之间文件被覆盖时服务端返回 412 使下载失败，
     *            避免按旧断点续读跳过新内容造成数据丢失，下轮扫描会按新 eTag 从头消费。
     * @return 惰性文件流。
     */
    public IOSupplier<InputStream> getObject(final String objectKey, final String expectedETag) {
        return () -> {
            final GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey);
            if (expectedETag != null) {
                requestBuilder.ifMatch(expectedETag);
            }
            try (InputStream s3Stream = s3Client.getObject(requestBuilder.build())) {
                return downloadToTempFile(s3Stream, downloadDir);
            }
        };
    }

    /**
     * 把输入流内容写入本地临时文件，返回读取该文件的流；返回的流关闭时自动删除临时文件。
     * 供 getObject 与无网络场景的测试复用。
     *
     * @param source
     *            数据源流（由调用方负责关闭）。
     * @param downloadDir
     *            临时文件目录，null 时使用系统临时目录。
     * @return 读取临时文件的流（close 时删除临时文件）。
     * @throws IOException
     *             on error.
     */
    public static InputStream downloadToTempFile(final InputStream source, final Path downloadDir)
            throws IOException {
        final Path tmp = downloadDir == null
                ? Files.createTempFile("s3-poller-", ".obj")
                : Files.createTempFile(downloadDir, "s3-poller-", ".obj");
        try {
            Files.copy(source, tmp, StandardCopyOption.REPLACE_EXISTING);
            final InputStream fileIn = Files.newInputStream(tmp);
            return new FilterInputStream(fileIn) {
                @Override
                public void close() throws IOException {
                    // 先关闭底层文件流释放句柄（Windows 下打开中的文件无法删除），再删除临时文件
                    super.close();
                    Files.deleteIfExists(tmp);
                }
            };
        } catch (final IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(tmp);
            } catch (final IOException ignored) {
                // 清理失败不影响原始异常
            }
            throw e;
        }
    }

    public void shutdown() {
        s3Client.close();
    }

    public void addPredicate(final Predicate<S3Object> objectPredicate) {
        this.filterPredicate = this.filterPredicate.and(objectPredicate);
    }

}
