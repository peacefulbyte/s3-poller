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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 独立轮询器入口：加载 YAML 配置 → 组装 {@link S3Poller} → 运行轮询循环。
 *
 * <p>
 * 用法：{@code java -cp <classpath> io.aiven.kafka.connect.s3.poller.PollerMain [poller.yaml]}
 * 未指定配置文件时默认使用当前目录下的 {@code poller.yaml}。
 *
 * <p>
 * 退出方式：Ctrl+C / SIGTERM 触发 shutdown hook，先停止轮询循环，
 * 再由 try-with-resources 关闭输出层与 S3 客户端；正在处理的批次完成后断点正常落盘。
 */
public final class PollerMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(PollerMain.class);

    /** shutdown hook 等待主循环退出的最长时间 */
    private static final long SHUTDOWN_GRACE_MS = TimeUnit.SECONDS.toMillis(30);

    private PollerMain() {
        // 工具类，禁止实例化
    }

    /**
     * 程序入口。
     *
     * @param args
     *            可选：配置文件路径（YAML）。
     * @throws Exception
     *             配置加载或轮询器组装失败。
     */
    public static void main(final String[] args) throws Exception {
        final Path configFile = Paths.get(args.length > 0 ? args[0] : "poller.yaml");
        LOGGER.info("加载配置: {}", configFile.toAbsolutePath());
        final PollerConfig config = PollerConfig.load(configFile);
        prepareDownloadDir(config);
        try (final S3Poller poller = S3Poller.create(config)) {
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // Ctrl+C / SIGTERM：请求停止轮询循环，等待主线程完成当前批次并落盘断点
                LOGGER.info("收到退出信号，正在停止轮询...");
                poller.stop();
                try {
                    mainThread.join(SHUTDOWN_GRACE_MS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "poller-shutdown"));
            poller.run();
        }
    }

    /**
     * 准备对象下载临时目录：仅当配置了专用目录时创建并清理上次进程崩溃残留的 s3-poller-*.obj；
     * 未配置（使用系统临时目录）时不做清理，避免误删其他实例正在下载的文件。
     *
     * @param config
     *            已加载的配置。
     * @throws IOException
     *             目录创建或清理失败。
     */
    private static void prepareDownloadDir(final PollerConfig config) throws IOException {
        if (StringUtils.isBlank(config.poll.downloadDir)) {
            return;
        }
        final Path downloadDir = Path.of(config.poll.downloadDir);
        Files.createDirectories(downloadDir);
        try (Stream<Path> files = Files.list(downloadDir)) {
            files.filter(PollerMain::isTempObj).forEach(PollerMain::deleteQuietly);
        }
    }

    /** 是否为下载临时文件（前缀 s3-poller-，后缀 .obj） */
    private static boolean isTempObj(final Path p) {
        final String name = p.getFileName().toString();
        return name.startsWith("s3-poller-") && name.endsWith(".obj");
    }

    /** 删除残留临时文件，失败仅告警不中断启动 */
    private static void deleteQuietly(final Path p) {
        try {
            Files.deleteIfExists(p);
            LOGGER.warn("已清理残留临时文件: {}", p);
        } catch (final IOException e) {
            LOGGER.warn("清理残留临时文件失败: {}", p, e);
        }
    }
}
