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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

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
}
