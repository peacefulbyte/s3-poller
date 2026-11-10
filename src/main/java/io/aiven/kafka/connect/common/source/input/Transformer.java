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

package io.aiven.kafka.connect.common.source.input;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.task.Context;

import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;

public abstract class Transformer {

    public final static long UNKNOWN_STREAM_LENGTH = -1;

    public final Stream<SchemaAndValue> getRecords(final IOSupplier<InputStream> inputStreamIOSupplier,
            final long streamLength, final Context<?> context, final SourceCommonConfig sourceConfig,
            final long skipRecords) {

        // 解析入口：按具体格式创建 Spliterator 逐条读取；skipRecords 用于从文件中段续读（断点恢复）
        final StreamSpliterator spliterator = createSpliterator(inputStreamIOSupplier, streamLength, context,
                sourceConfig);
        return StreamSupport.stream(spliterator, false).onClose(spliterator::close).skip(skipRecords);
    }

    /**
     * Creates the stream spliterator for this transformer.
     *
     * @param inputStreamIOSupplier
     *            the input stream supplier.
     * @param streamLength
     *            the length of the input stream, {@link #UNKNOWN_STREAM_LENGTH} may be used to specify a stream with an
     *            unknown length, streams of length zero will log an error and return an empty stream
     * @param context
     *            the context
     * @param sourceConfig
     *            the source configuration.
     * @return a StreamSpliterator instance.
     */
    protected abstract StreamSpliterator createSpliterator(IOSupplier<InputStream> inputStreamIOSupplier,
            long streamLength, Context<?> context, SourceCommonConfig sourceConfig);

    public abstract SchemaAndValue getKeyData(Object cloudStorageKey, String topic, SourceCommonConfig sourceConfig);

    /**
     * A Spliterator that performs various checks on the opening/closing of the input stream.
     */
    protected abstract static class StreamSpliterator implements Spliterator<SchemaAndValue> {
        /**
         * The input stream supplier.
         */
        private final IOSupplier<InputStream> inputStreamIOSupplier;
        /**
         * The logger to be used by all instances of this class. This will be the Transformer logger.
         */
        protected final Logger logger;
        /**
         * * The input stream. Will be null until {@link #inputOpened} has completed. May be used for reading but should
         * not be closed or otherwise made unreadable.
         */
        protected InputStream inputStream;

        /**
         * A flag indicate that the input stream has been closed.
         */
        private boolean closed;

        /**
         * Constructor.
         *
         * @param logger
         *            The logger for this Spliterator to use.
         * @param inputStreamIOSupplier
         *            the InputStream supplier
         */
        protected StreamSpliterator(final Logger logger, final IOSupplier<InputStream> inputStreamIOSupplier) {
            this.logger = logger;
            this.inputStreamIOSupplier = inputStreamIOSupplier;
        }

        /**
         * 读取下一条记录；只有正常 EOF 返回 false，读取或解析异常必须向上传播。
         * 产出记录时调用 {@code action.accept()}。
         *
         * @param action
         *            the Consumer to call if record is created.
         * @return {@code true} if a record was processed, {@code false} otherwise.
         */
        abstract protected boolean doAdvance(Consumer<? super SchemaAndValue> action);

        /**
         * Method to close additional inputs if needed.
         */
        abstract protected void doClose();

        public final void close() {
            if (closed) {
                return;
            }
            closed = true;
            final InputStream opened = inputStream;
            inputStream = null;
            try (opened) {
                doClose();
            } catch (IOException e) {
                throw new UncheckedIOException("关闭对象输入流失败", e);
            }
        }

        private void closeOnFailure(final Throwable failure) {
            try {
                close();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
        }

        /**
         * Allows modification of input stream. Called immediatly after the input stream is opened. Implementations may
         * modify the type of input stream by wrapping it with a specific implementation, or may create Readers from the
         * input stream. The modified input stream must be returned. If a Reader or similar class is created from the
         * input stream the input stream must be returned. The {@link #inputStream} instance variable will be null until
         * {@link #inputOpened} has completed. The implementation of the interface is responsible for closing any newly
         * constructed readers or input streams in the doClose() method.
         *
         * @param input
         *            the input stream that was just opened.
         * @throws IOException
         *             on IO error.
         */
        abstract protected void inputOpened(InputStream input) throws IOException;

        @Override
        public final boolean tryAdvance(final Consumer<? super SchemaAndValue> action) {
            if (closed) {
                return false;
            }
            try {
                if (inputStream == null) {
                    inputStream = inputStreamIOSupplier.get();
                    inputOpened(inputStream);
                }
                final boolean result = doAdvance(action);
                if (!result) {
                    close();
                }
                return result;
            } catch (IOException e) {
                final UncheckedIOException failure = new UncheckedIOException("打开对象输入流失败", e);
                closeOnFailure(failure);
                throw failure;
            } catch (RuntimeException | Error e) {
                closeOnFailure(e);
                throw e;
            }
        }

        @Override
        public final Spliterator<SchemaAndValue> trySplit() { // NOPMD returning null is reqruied by API
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return Spliterator.ORDERED | Spliterator.NONNULL;
        }
    }
}
