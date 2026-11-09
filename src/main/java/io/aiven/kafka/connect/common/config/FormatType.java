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

package io.aiven.kafka.connect.common.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

public enum FormatType {
    /** Handles in Avro format */
    AVRO("avro"),
    /** Handles in CSV format */
    CSV("csv"),
    /** Handles in JSON format */
    JSON("json"),
    /** Handles in JSONL format */
    JSONL("jsonl"),
    /** Handles Parquet format */
    PARQUET("parquet");

    /**
     * A list of supported format types for display.
     */
    public static final String SUPPORTED_FORMAT_TYPES = FormatType.names()
            .stream()
            .map(c -> String.format("'%s'", c))
            .collect(Collectors.joining(", "));

    /**
     * The name of this type.
     */
    public final String name;

    /**
     * Constructor.
     *
     * @param name
     *            The name of this type.
     */
    FormatType(final String name) {
        this.name = name;
    }

    /**
     * Gets the format type by name.
     *
     * @param name
     *            the name to lookup.
     * @return the FormatType
     * @throws IllegalArgumentException
     *             if not found
     */
    public static FormatType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        try {
            return FormatType.valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            throw new IllegalArgumentException("Unknown format type: " + name);
        }
    }

    /**
     * Gets the collection of names.
     *
     * @return the collection of names.
     */
    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }
}
