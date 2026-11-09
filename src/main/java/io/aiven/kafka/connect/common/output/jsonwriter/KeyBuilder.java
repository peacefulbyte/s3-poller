/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.common.output.jsonwriter;

import java.io.IOException;
import java.util.Objects;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.JsonNode;

class KeyBuilder implements OutputFieldBuilder {

    @Override
    public JsonNode build(final SinkRecord record) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        if (record.key() == null) {
            return null;
        }

        // 替代 JsonConverter.fromConnectData：sink 死代码，仅保持编译，直接按值转 JsonNode
        return ObjectMapperProvider.get().valueToTree(record.key());
    }
}
