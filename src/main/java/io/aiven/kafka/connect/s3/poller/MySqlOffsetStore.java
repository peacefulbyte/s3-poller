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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 MySQL 8.0 单表的断点存储实现。
 *
 * <p>
 * 表结构（启动时幂等创建，缺省 {@code s3_offset}）：
 * <ul>
 * <li>{@code is_current=1} 的行 = 最后已确认的记录断点，对象可能尚未完成</li>
 * <li>全部行按自增 {@code id} 排序 = 进度提交顺序；窗口包含当前对象的 eTag，
 * 恢复完成去重集合时必须排除当前对象，由轮询器先校验版本并续读</li>
 * <li>{@code uk_bucket_key_hash (bucket, key_hash)} 唯一键；{@code key_hash} 为 object_key 的
 * SHA-256 十六进制，避免 utf8mb4 下 bucket+object_key 复合索引超 InnoDB 3072 字节上限；
 * 覆盖重消费时先删后插，新行 id 排到窗口尾部，与文件版 remove+append 语义一致</li>
 * </ul>
 *
 * <p>
 * {@link #save} 同一对象内推进断点时单条 UPDATE 原地更新 record_count/eTag；
 * 换对象或首次保存时四步（降级旧断点、删旧行、插新行、裁剪窗口）在单事务内完成，
 * 对应文件版的「临时文件 + 原子移动」。
 *
 * <p>
 * 失败语义：连接失败 / SQL 失败抛 {@link IllegalStateException}。构造时建表失败或
 * {@link #load()} 失败都会使启动失败（fail-fast），与文件版「损坏回退桶首」不同：
 * 数据库不可达时回退桶首会导致全量重扫，应让运维直接感知。
 */
public final class MySqlOffsetStore implements OffsetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlOffsetStore.class);

    /** 表名只允许字母/数字/下划线，防止拼接注入 */
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

    /** SHA-256 十六进制字符表 */
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final String bucket;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    /** 断点表名（已通过格式校验，拼接 SQL 安全） */
    private final String table;
    private final int maxProcessedKeys;

    /**
     * 构造 MySQL 断点存储并幂等建表。
     *
     * @param bucket
     *            存储桶名，表内按桶隔离多桶数据。
     * @param jdbcUrl
     *            JDBC 连接串。
     * @param username
     *            用户名。
     * @param password
     *            密码，可为 {@code null}。
     * @param table
     *            断点表名，只允许字母/数字/下划线。
     * @param maxProcessedKeys
     *            已消费窗口的最大保留行数，超过后裁剪最旧的。
     * @throws IllegalArgumentException
     *             表名不合法。
     * @throws IllegalStateException
     *             数据库不可达或建表失败（启动应 fail-fast）。
     */
    public MySqlOffsetStore(final String bucket, final String jdbcUrl, final String username, final String password,
            final String table, final int maxProcessedKeys) {
        this.bucket = bucket;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        if (!TABLE_NAME_PATTERN.matcher(table).matches()) {
            throw new IllegalArgumentException("非法断点表名（只允许字母/数字/下划线）: " + table);
        }
        this.table = table;
        this.maxProcessedKeys = Math.max(1, maxProcessedKeys);
        ensureTable();
    }

    /**
     * 按轮询器配置创建 MySQL 断点存储。
     *
     * @param config
     *            已校验的 YAML 配置。
     * @return 就绪的 MySQL 断点存储。
     */
    public static MySqlOffsetStore create(final PollerConfig config) {
        final PollerConfig.MySql mysql = config.offsets.mysql;
        final int max = mysql.maxProcessedKeys != null ? mysql.maxProcessedKeys : config.poll.ringBufferSize;
        return new MySqlOffsetStore(config.s3.bucket, mysql.jdbcUrl, mysql.username, mysql.password, mysql.table,
                max);
    }

    /**
     * 读取断点。断点行不存在时返回空（从桶首开始消费）；
     * 窗口行与断点行一并返回（断点行必然在窗口尾部，与文件版一致）。
     */
    @Override
    public Optional<Offset> load() {
        try (Connection conn = connect()) {
            // 最近断点行
            final Optional<Object[]> current = queryOne(conn,
                    "SELECT object_key, record_count FROM `" + table
                            + "` WHERE bucket = ? AND is_current = 1",
                    stmt -> stmt.setString(1, bucket));
            if (current.isEmpty()) {
                LOGGER.info("MySQL 断点表无断点记录，将从桶首开始消费: bucket={}", bucket);
                return Optional.empty();
            }
            // 最近 N 个提交过进度的 key→eTag，含可能未完成的当前断点，由轮询器拆分恢复状态。
            final Map<String, String> keys = new LinkedHashMap<>();
            query(conn,
                    "SELECT object_key, e_tag FROM (SELECT id, object_key, e_tag FROM `" + table
                            + "` WHERE bucket = ? ORDER BY id DESC LIMIT " + maxProcessedKeys
                            + ") w ORDER BY id ASC",
                    stmt -> stmt.setString(1, bucket), rs -> keys.put(rs.getString(1), rs.getString(2)));
            return Optional.of(
                    new Offset(bucket, (String) current.get()[0], (long) current.get()[1], keys));
        } catch (final SQLException e) {
            throw new IllegalStateException("读取 MySQL 断点失败: bucket=" + bucket, e);
        }
    }

    /**
     * 保存断点。
     *
     * <p>
     * 同一对象推进时（当前断点行即该 key）：单条 UPDATE 原地更新 record_count/eTag，
     * 行保持在窗口尾部；换对象或首次保存时走四步单事务：
     * 降级旧断点 → 删除同 key 旧行 → 插入新断点行 → 裁剪窗口。
     */
    @Override
    public void save(final String bucket, final String objectKey, final long recordCount, final String eTag) {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try {
                // 同一对象推进断点：当前行就是该 key，原地更新即可，避免删插行放大写
                if (exec(conn,
                        "UPDATE `" + table
                                + "` SET record_count = ?, e_tag = ?, consumed_at = CURRENT_TIMESTAMP(3) "
                                + "WHERE bucket = ? AND is_current = 1 AND key_hash = ?",
                        stmt -> {
                            stmt.setLong(1, recordCount);
                            if (eTag != null) {
                                stmt.setString(2, eTag);
                            } else {
                                stmt.setNull(2, Types.VARCHAR);
                            }
                            stmt.setString(3, bucket);
                            stmt.setString(4, sha256Hex(objectKey));
                        }) > 0) {
                    conn.commit();
                    return;
                }
                // 换对象或首次保存：完整路径
                // 1. 旧断点降级（每桶最多一行 is_current=1）
                exec(conn, "UPDATE `" + table + "` SET is_current = 0 WHERE bucket = ? AND is_current = 1",
                        stmt -> stmt.setString(1, bucket));
                // 2. 覆盖重消费时删除旧行（key_hash 匹配），保证新行 id 排到窗口尾部
                exec(conn, "DELETE FROM `" + table + "` WHERE bucket = ? AND key_hash = ?", stmt -> {
                    stmt.setString(1, bucket);
                    stmt.setString(2, sha256Hex(objectKey));
                });
                // 3. 插入新断点行
                exec(conn,
                        "INSERT INTO `" + table
                                + "` (bucket, object_key, key_hash, e_tag, record_count, is_current) "
                                + "VALUES (?, ?, ?, ?, ?, 1)",
                        stmt -> {
                            stmt.setString(1, bucket);
                            stmt.setString(2, objectKey);
                            stmt.setString(3, sha256Hex(objectKey));
                            if (eTag != null) {
                                stmt.setString(4, eTag);
                            } else {
                                stmt.setNull(4, Types.VARCHAR);
                            }
                            stmt.setLong(5, recordCount);
                        });
                // 4. 窗口裁剪：仅保留最近 N 行（断点行 id 必然最新，不会被裁）
                final Long minId = queryLong(conn,
                        "SELECT MIN(id) FROM (SELECT id FROM `" + table + "` WHERE bucket = ? ORDER BY id DESC LIMIT "
                                + maxProcessedKeys + ") t",
                        stmt -> stmt.setString(1, bucket));
                if (minId != null) {
                    exec(conn,
                            "DELETE FROM `" + table + "` WHERE bucket = ? AND is_current = 0 AND id < ?",
                            stmt -> {
                                stmt.setString(1, bucket);
                                stmt.setLong(2, minId);
                            });
                }
                conn.commit();
            } catch (final SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException("保存 MySQL 断点失败: bucket=" + bucket + ", key=" + objectKey, e);
        }
    }

    /** 幂等建表；失败抛 IllegalStateException，启动 fail-fast */
    private void ensureTable() {
        try (Connection conn = connect(); PreparedStatement stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS `"
                + table + "` (\n"
                + "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，表达消费先后顺序',\n"
                + "  bucket VARCHAR(255) NOT NULL COMMENT '存储桶名',\n"
                + "  object_key VARCHAR(1024) NOT NULL COMMENT 'S3 对象 key',\n"
                + "  key_hash CHAR(64) NOT NULL COMMENT 'object_key 的 SHA-256 十六进制，参与唯一键（避免长复合索引超限）',\n"
                + "  e_tag VARCHAR(255) NULL COMMENT '消费时的 eTag；NULL=旧断点无 eTag 信息',\n"
                + "  record_count BIGINT NOT NULL DEFAULT 0 COMMENT '对象内已消费记录数（仅断点对象有意义）',\n"
                + "  is_current TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1=最近断点对象，每桶最多一行',\n"
                + "  consumed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '最后确认时间',\n"
                + "  PRIMARY KEY (id),\n"
                + "  UNIQUE KEY uk_bucket_key_hash (bucket, key_hash),\n"
                + "  KEY idx_bucket_current (bucket, is_current)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = 's3-poller 断点存储'")) {
            stmt.execute();
        } catch (final SQLException e) {
            throw new IllegalStateException("创建 MySQL 断点表失败: " + table, e);
        }
        LOGGER.info("MySQL 断点表就绪: {}, 窗口上限 {}", table, maxProcessedKeys);
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    /** object_key 的 SHA-256 十六进制（64 字符 ASCII），用于唯一键匹配 */
    private String sha256Hex(final String objectKey) {
        try {
            final byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(objectKey.getBytes(StandardCharsets.UTF_8));
            final StringBuilder hex = new StringBuilder(digest.length * 2);
            for (final byte b : digest) {
                hex.append(HEX[(b >> 4) & 0xF]).append(HEX[b & 0xF]);
            }
            return hex.toString();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }

    /** 执行更新/删除/插入，返回受影响行数 */
    private int exec(final Connection conn, final String sql, final StmtSetter setter) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            setter.apply(stmt);
            return stmt.executeUpdate();
        }
    }

    /** 查询单行两列（object_key, record_count），无行返回空 */
    private Optional<Object[]> queryOne(final Connection conn, final String sql, final StmtSetter setter)
            throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            setter.apply(stmt);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new Object[] { rs.getString(1), rs.getLong(2) });
                }
                return Optional.empty();
            }
        }
    }

    /** 查询单行单列（MIN(id)），无行返回 null */
    private Long queryLong(final Connection conn, final String sql, final StmtSetter setter) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            setter.apply(stmt);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }
        }
    }

    /** 遍历查询结果 */
    private void query(final Connection conn, final String sql, final StmtSetter setter, final RowConsumer consumer)
            throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            setter.apply(stmt);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    consumer.accept(rs);
                }
            }
        }
    }

    @FunctionalInterface
    private interface StmtSetter {
        void apply(PreparedStatement stmt) throws SQLException;
    }

    @FunctionalInterface
    private interface RowConsumer {
        void accept(ResultSet rs) throws SQLException;
    }
}
