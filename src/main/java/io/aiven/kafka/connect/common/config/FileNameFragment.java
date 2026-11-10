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

package io.aiven.kafka.connect.common.config;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import io.aiven.kafka.connect.common.config.validators.SourcenameTemplateValidator;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.aiven.kafka.connect.common.templating.Template;

/**
 * Fragment to handle all file name extraction operations.
 */
public final class FileNameFragment extends ConfigFragment {

    // package private so that testing can access.
    static final String GROUP_FILE = "File";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    static final String FILE_MAX_RECORDS = "file.max.records";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";
    static final String DEFAULT_FILENAME_TEMPLATE = "{{topic}}-{{partition}}-{{start_offset}}";

    public static final String FILE_PATH_PREFIX_TEMPLATE_CONFIG = "file.prefix.template";

    /**
     * Gets a setter for this properties in this fragment.
     *
     * @param data
     *            the data to update.
     * @return the Setter.
     */
    public static Setter setter(final Map<String, String> data) {
        return new Setter(data);
    }

    /**
     * Create an instance of this fragment wrapping the specified config.
     *
     * @param cfg
     *            the configuration to read from.
     */
    public FileNameFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    /**
     * Validate the distribution type works with the file name template.
     *
     * @param distributionType
     *            the distribution type for the validator
     */
    public void validateDistributionType(final DistributionType distributionType) {
        new SourcenameTemplateValidator(distributionType).ensureValid(FILE_NAME_TEMPLATE_CONFIG, getSourceName());
    }

    /**
     * Adds the FileName properties to the configuration definition.
     *
     * @param configDef
     *            the configuration definition to update.
     * @return the updated configuration definition.
     */
    public static ConfigDef update(final ConfigDef configDef) {
        return update(configDef, CompressionType.NONE);
    }

    /**
     * Adds the FileName properties to the configuration definition.
     *
     * @param configDef
     *            the configuration definition to update.
     * @param defaultCompressionType
     *            The default compression type. May be {@code null}.
     * @return the updated configuration definition.
     */
    public static ConfigDef update(final ConfigDef configDef, final CompressionType defaultCompressionType) {
        int fileGroupCounter = 0;

        configDef.define(FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.MEDIUM,
                "The template for file names on storage system. "
                        + "Supports `{{ variable }}` placeholders for substituting variables. "
                        + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                        + "(the offset of the first record in the file). "
                        + "Only some combinations of variables are valid, which currently are:\n"
                        + "- `topic`, `partition`, `start_offset`."
                        + "There is also `key` only variable {{key}} for grouping by keys",
                GROUP_FILE, ++fileGroupCounter, ConfigDef.Width.LONG, FILE_NAME_TEMPLATE_CONFIG);

        configDef.define(FILE_PATH_PREFIX_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null, null,
                ConfigDef.Importance.MEDIUM,
                "The template for file names prefixes on storage system. "
                        + "Supports `{{ variable }}` placeholders for substituting variables. "
                        + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                        + "(the offset of the first record in the file). "
                        + "Only some combinations of variables are valid, which currently are:\n"
                        + "- `topic`, `partition`, `start_offset`."
                        + "There is also `key` only variable {{key}} for grouping by keys",
                GROUP_FILE, ++fileGroupCounter, ConfigDef.Width.LONG, FILE_PATH_PREFIX_TEMPLATE_CONFIG);

        configDef.define(FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, null, new FileCompressionTypeValidator(),
                ConfigDef.Importance.MEDIUM, "The compression type used for files.", GROUP_FILE, fileGroupCounter++,
                ConfigDef.Width.NONE, FILE_COMPRESSION_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(CompressionType.names()));

        configDef.define(FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, ConfigDef.Range.between(0, Integer.MAX_VALUE),
                ConfigDef.Importance.MEDIUM,
                "The maximum number of records to put in a single file. " + "Must be a non-negative integer number. "
                        + "0 is interpreted as \"unlimited\", which is the default.",
                GROUP_FILE, ++fileGroupCounter, ConfigDef.Width.SHORT, FILE_MAX_RECORDS);

        return configDef;
    }

    @Override
    public void validate() {
        throw new ConfigException("Do not call FileNameFragment.validate() -- call sink/source specific validator");
    }

    /**
     * Returns the text of the filename template. May throw {@link ConfigException} if the property
     * {@link #FILE_NAME_TEMPLATE_CONFIG} is not set.
     *
     * @return the text of the filename template.
     */
    public String getFilename() {
        return cfg.getString(FILE_NAME_TEMPLATE_CONFIG);
    }

    /**
     * Retrieves the defined compression type.
     *
     * @return the defined compression type or {@link CompressionType#NONE} if there is no defined compression type.
     */
    public CompressionType getCompressionType() {
        return has(FILE_COMPRESSION_TYPE_CONFIG)
                ? CompressionType.forName(cfg.getString(FILE_COMPRESSION_TYPE_CONFIG))
                : CompressionType.NONE;
    }

    /**
     * Gets the filename template.
     *
     * @return the Filename template.
     */
    public Template getFilenameTemplate() {
        return Template.of(getFilename());
    }

    /**
     * Gets the maximum number of records allowed in a file.
     *
     * @return the maximum number of records allowed in a file.
     */
    public int getMaxRecordsPerFile() {
        return cfg.getInt(FILE_MAX_RECORDS);
    }

    public String getSourceName() {
        return cfg.getString(FILE_NAME_TEMPLATE_CONFIG);
    }

    public String getPrefixTemplate() {
        return cfg.getString(FILE_PATH_PREFIX_TEMPLATE_CONFIG);
    }

    /**
     * Setter for the FileNameFragment.
     */
    public static final class Setter extends AbstractFragmentSetter<Setter> {
        /**
         * Constructs the Setter.
         *
         * @param data
         *            the data to update.
         */
        private Setter(final Map<String, String> data) {
            super(data);
        }

        /**
         * Sets the file compression type.
         *
         * @param compressionType
         *            the compression type.
         * @return this
         */
        public Setter fileCompression(final CompressionType compressionType) {
            return setValue(FILE_COMPRESSION_TYPE_CONFIG, compressionType.name());
        }

        /**
         * Sets the maximum records per file.
         *
         * @param maxRecordsPerFile
         *            the maximum records per file.
         * @return this.
         */
        public Setter maxRecordsPerFile(final int maxRecordsPerFile) {
            return setValue(FILE_MAX_RECORDS, maxRecordsPerFile);
        }

        /**
         * Sets the file name template.
         *
         * @param template
         *            the prefix template to use.
         * @return this.
         */
        public Setter template(final String template) {
            return setValue(FILE_NAME_TEMPLATE_CONFIG, template);
        }

        /**
         * Sets the file name prefix template.
         *
         * @param prefixTemplate
         *            the prefix template to use.
         * @return this
         */
        public Setter prefixTemplate(final String prefixTemplate) {
            return setValue(FILE_PATH_PREFIX_TEMPLATE_CONFIG, prefixTemplate);
        }
    }
}
