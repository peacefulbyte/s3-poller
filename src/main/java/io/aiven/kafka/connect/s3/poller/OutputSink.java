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

import io.aiven.kafka.connect.common.source.PollerRecord;

/**
 * 输出层统一接口：消费 {@link PollerRecord} 并写入目标。
 *
 * <p>
 * 两个实现：{@link FileOutputSink}（本地文件，测试）与 KafkaOutputSink（Kafka，生产）。
 * 轮询器主循环只依赖本接口，切换输出方式无需改动消费逻辑。
 *
 * <p>
 * 写入语义：{@link #write} 可能只是入队（如 Kafka 异步发送），
 * 调用方须在每个批次结束时调用 {@link #flush} 确保数据真正落盘/发出。
 * {@link #flush} 抛出 IOException 表示本批次存在未送达的记录，
 * 调用方此时不应推进断点，重启后可整批重发（at-least-once）。
 */
public interface OutputSink extends AutoCloseable {

    /**
     * 写入一条记录。
     *
     * @param record
     *            要输出的记录。
     * @throws IOException
     *             目标不可写时抛出。
     */
    void write(PollerRecord record) throws IOException;

    /**
     * 冲刷并确认本批次记录已送达。
     *
     * @throws IOException
     *             存在未能送达的记录时抛出。
     */
    void flush() throws IOException;

    @Override
    void close() throws IOException;
}
