package io.aiven.kafka.connect.common.source.input;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.task.Context;

import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ByteLineTransformer 按字节把对象的每一行切分为一条记录透传，不做任何字符解码或内容解析。
 *
 * <p>
 * 行边界按字节扫描 {@code 0x0A}（LF）确定，与编码无关：UTF-8 / GBK / Big5 等
 * ASCII 兼容编码的多字节字符中均不含 {@code 0x0A}，不会被误切。
 * 每条记录的 value 为该行的原始字节（schema 为 null），行尾 {@code \r\n} / {@code \n}
 * 被剥离，空行被跳过；文件末尾无换行符时，剩余内容作为最后一行输出。
 * 消费端负责按实际编码解码（如 {@code new String(value, charset)}）。
 *
 * <p>
 * 单行最大字节数受 {@code transformer.max.buffer.size} 限制（复用 bytes 的切块大小配置），
 * 超过上限立即抛错停止，保留断点供调整上限后重试，不丢弃或截断记录。
 */
public class ByteLineTransformer extends Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ByteLineTransformer.class);

    private static final byte LF = 0x0A;
    private static final byte CR = 0x0D;

    /** 每次从底层流读入的字节缓冲大小，与单行上限无关 */
    private static final int READ_CHUNK = 8192;

    @Override
    public StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
            final long streamLength, final Context<?> context, final SourceCommonConfig sourceConfig) {
        if (streamLength == 0) {
            LOGGER.warn(
                    "Object sent for processing has an invalid streamLength of {}, object is empty returning an empty spliterator.",
                    streamLength);
            return emptySpliterator(inputStreamIOSupplier);
        }
        // 单行有界缓冲：超限抛错，避免丢弃记录或无限占用内存。
        final int maxLineBytes = sourceConfig.getTransformerMaxBufferSize();
        return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {

            /** 当前行的字节缓冲 */
            private final ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream();
            /** 底层流读缓冲，跨 doAdvance 调用复用，避免未消费字节丢失 */
            private final byte[] buffer = new byte[READ_CHUNK];
            /** 读缓冲中已消费到的位置 */
            private int bufferPos = 0;
            /** 读缓冲中的有效字节数 */
            private int bufferLen = 0;

            @Override
            protected void inputOpened(final InputStream input) {
                // nothing to do.
            }

            @Override
            protected void doClose() {
                // nothing to do.
            }

            @Override
            protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
                try {
                    while (true) {
                        // 缓冲已消费完：从底层流读入下一段
                        if (bufferPos >= bufferLen) {
                            final int read = inputStream.read(buffer);
                            if (read == -1) {
                                // EOF：文件末尾无换行符时，缓冲中剩余内容作为最后一行
                                final byte[] line = finishLine();
                                if (line == null) {
                                    return false;
                                }
                                action.accept(new SchemaAndValue(null, line));
                                return true;
                            }
                            bufferPos = 0;
                            bufferLen = read;
                        }
                        // 扫描读缓冲中的字节，直到产出一行或缓冲耗尽
                        while (bufferPos < bufferLen) {
                            if (buffer[bufferPos] == LF) {
                                bufferPos++;
                                // 一行结束：剥离行尾 CR 后输出；空行跳过继续找下一行
                                final byte[] line = finishLine();
                                if (line != null) {
                                    action.accept(new SchemaAndValue(null, line));
                                    return true;
                                }
                                break;
                            } else {
                                if (lineBuffer.size() >= maxLineBytes) {
                                    throw new IllegalStateException("行超过最大长度 " + maxLineBytes
                                            + " 字节，停止解析；调整 maxBufferSize 后重试");
                                }
                                lineBuffer.write(buffer[bufferPos]);
                                bufferPos++;
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("读取字节行失败", e);
                }
            }

            /** 取出当前缓冲中的一行：剥离行尾 CR；空行返回 null */
            private byte[] finishLine() {
                final byte[] line = lineBuffer.toByteArray();
                lineBuffer.reset();
                int end = line.length;
                if (end > 0 && line[end - 1] == CR) {
                    end--;
                }
                return end > 0 ? Arrays.copyOf(line, end) : null;
            }
        };
    }

    /**
     * This method returns an empty spliterator when an empty input stream is supplied to be split.
     *
     * @param inputStreamIOSupplier
     *            The empty input stream that was supplied.
     * @return an Empty spliterator to return to the calling method.
     */
    private static StreamSpliterator emptySpliterator(final IOSupplier<InputStream> inputStreamIOSupplier) {
        return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {
            @Override
            protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
                return false;
            }

            @Override
            protected void doClose() {
                // nothing to do
            }

            @Override
            protected void inputOpened(final InputStream input) {
                // nothing to do
            }

        };
    }

    @Override
    public SchemaAndValue getKeyData(final Object cloudStorageKey, final String topic,
            final SourceCommonConfig sourceConfig) {
        return new SchemaAndValue(null, ((String) cloudStorageKey).getBytes(StandardCharsets.UTF_8));
    }
}
