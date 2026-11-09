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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.common.source.input.InputFormat;

import io.confluent.connect.avro.AvroData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

/**
 * 记录值编码器：把解析结果 {@link SchemaAndValue} 编码为输出字节，供文件/Kafka 输出复用。
 *
 * <ul>
 * <li>{@code JSONL}：Connect 值转 JSON 文本（UTF-8），每行一条记录</li>
 * <li>{@code BYTES}/{@code BYTES_LINE}：原样字节透传（bytes-line 每行一条）</li>
 * <li>{@code AVRO}/{@code PARQUET}：Connect 值经 {@link AvroData} 反转成 Avro 对象后，
 * 编码为<b>裸 Avro 二进制</b>（每条记录独立编码，不含 schema 头，消费端需已知 schema）</li>
 * </ul>
 *
 * <p>
 * 线程安全：实例无状态字段被修改（AvroData 自带缓存），可在多线程输出中共享。
 */
public final class RecordValueEncoder {

    /** Connect 类型与 Avro 类型互转（沿用 Transformer 相同的缓存大小） */
    private final AvroData avroData;
    private final ObjectMapper mapper;

    public RecordValueEncoder() {
        this.avroData = new AvroData(100);
        this.mapper = new ObjectMapper();
    }

    /**
     * 按输入格式编码 value。
     *
     * @param valueData
     *            解析出的 value（schema + 值）。
     * @param inputFormat
     *            输入格式，决定编码方式。
     * @return 编码后的字节。
     * @throws IOException
     *             编码失败。
     */
    public byte[] encodeValue(final SchemaAndValue valueData, final InputFormat inputFormat) throws IOException {
        switch (inputFormat) {
            case JSONL :
                return encodeJson(valueData);
            case BYTES :
            case BYTES_LINE :
                // 原样字节透传：不解码、不转码，消费端自行按实际编码解码
                return (byte[]) valueData.value();
            case AVRO :
            case PARQUET :
                return encodeAvro(valueData);
            default :
                throw new IllegalArgumentException("不支持的输入格式: " + inputFormat);
        }
    }

    /**
     * Connect 值转 JSON 文本。
     *
     * @param valueData
     *            解析出的 value。
     * @return JSON 文本字节（UTF-8）。
     * @throws IOException
     *             序列化失败。
     */
    public byte[] encodeJson(final SchemaAndValue valueData) throws IOException {
        return mapper.writeValueAsBytes(toJsonNode(valueData.schema(), valueData.value()));
    }

    /**
     * Connect 值转裸 Avro 二进制。
     *
     * @param valueData
     *            解析出的 value（须携带完整 Connect Schema）。
     * @return 单条记录的 Avro 二进制编码。
     * @throws IOException
     *             编码失败。
     */
    public byte[] encodeAvro(final SchemaAndValue valueData) throws IOException {
        final org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(valueData.schema());
        final Object avroValue = avroData.fromConnectData(valueData.schema(), valueData.value());
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        final DatumWriter<Object> writer = new GenericDatumWriter<>(avroSchema);
        writer.write(avroValue, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    /**
     * 把 Connect 类型值递归转换为 Jackson JsonNode。
     * schema 为 null 时（JsonConverter schemas.enable=false 产出）值即原生 Java 类型，直接映射。
     */
    private JsonNode toJsonNode(final Schema schema, final Object value) {
        if (value == null) {
            return NullNode.getInstance();
        }
        if (schema == null) {
            // JSONL（schemas.enable=false）：Map/List/字符串/数字等原生类型
            return mapper.valueToTree(value);
        }
        switch (schema.type()) {
            case INT8 :
                return ShortNode.valueOf(((Byte) value).shortValue());
            case INT16 :
                return ShortNode.valueOf((Short) value);
            case INT32 :
                return IntNode.valueOf((Integer) value);
            case INT64 :
                return LongNode.valueOf((Long) value);
            case FLOAT32 :
                return FloatNode.valueOf((Float) value);
            case FLOAT64 :
                return DoubleNode.valueOf((Double) value);
            case BOOLEAN :
                return BooleanNode.valueOf((Boolean) value);
            case STRING :
                return TextNode.valueOf((String) value);
            case BYTES :
                return BinaryNode.valueOf((byte[]) value);
            case ARRAY : {
                final ArrayNode array = mapper.createArrayNode();
                final List<?> list = (List<?>) value;
                list.forEach(item -> array.add(toJsonNode(schema.valueSchema(), item)));
                return array;
            }
            case MAP : {
                if (schema.keySchema().type() == Schema.Type.STRING) {
                    final ObjectNode object = mapper.createObjectNode();
                    ((Map<?, ?>) value)
                            .forEach((k, v) -> object.set(String.valueOf(k), toJsonNode(schema.valueSchema(), v)));
                    return object;
                }
                // 非字符串 key：降级为 [{key, value}] 数组
                final ArrayNode array = mapper.createArrayNode();
                ((Map<?, ?>) value).forEach((k, v) -> {
                    final ObjectNode pair = mapper.createObjectNode();
                    pair.set("key", toJsonNode(schema.keySchema(), k));
                    pair.set("value", toJsonNode(schema.valueSchema(), v));
                    array.add(pair);
                });
                return array;
            }
            case STRUCT : {
                final ObjectNode object = mapper.createObjectNode();
                final Struct struct = (Struct) value;
                schema.fields().forEach(field -> object.set(field.name(), toJsonNode(field.schema(), struct.get(field))));
                return object;
            }
            default :
                throw new IllegalArgumentException("不支持的 Connect 类型: " + schema.type());
        }
    }
}
