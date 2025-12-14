package com.zhugeio.etl.common.sink;

import java.io.Serializable;
import java.util.Properties;

/**
 * Doris Sink 配置
 *
 * 支持灵活配置各种场景的Sink
 */
public class DorisSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // ========== 连接配置 ==========
    private String feNodes;           // FE节点: "fe1:8030,fe2:8030"
    private String username;          // 用户名
    private String password;          // 密码
    private String database;          // 数据库名
    private String table;             // 表名

    // ========== 批次配置 ==========
    private int batchSize;            // 批次大小
    private long batchIntervalMs;     // 批次间隔(毫秒)
    private int maxRetries;           // 最大重试次数

    // ========== 部分列更新配置 ==========
    private boolean partialUpdate;    // 是否启用部分列更新
    private String mergeType;         // MERGE类型: MERGE, APPEND, DELETE

    // ========== Stream Load 配置 ==========
    private String format;            // 格式: json, csv
    private boolean readJsonByLine;   // 按行读取JSON
    private boolean stripOuterArray;  // 去除外层数组
    private String maxFilterRatio;    // 最大过滤比例
    private String timeout;           // 超时时间(秒)
    private boolean strictMode;       // 严格模式
    private String loadMemLimit;      // 内存限制(字节)

    // ========== Label 配置 ==========
    private String labelPrefix;       // Label前缀

    // ========== 序列化配置 ==========
    private String serializerType;    // 序列化器类型: jackson, fastjson
    private String includeStrategy;   // 包含策略: NON_NULL, NON_EMPTY

    // ========== 构造器 ==========

    private DorisSinkConfig() {
    }

    public static Builder builder() {
        return new Builder();
    }

    // ========== Builder ==========

    public static class Builder {
        private DorisSinkConfig config;

        public Builder() {
            config = new DorisSinkConfig();
            // 设置默认值
            config.batchSize = 10000;
            config.batchIntervalMs = 10000;
            config.maxRetries = 3;
            config.partialUpdate = false;
            config.mergeType = "APPEND";
            config.format = "json";
            config.readJsonByLine = true;
            config.stripOuterArray = true;
            config.maxFilterRatio = "0.1";
            config.timeout = "600";
            config.strictMode = false;
            config.loadMemLimit = "4294967296";  // 4GB
            config.labelPrefix = "flink-etl";
            config.serializerType = "jackson";
            config.includeStrategy = "NON_NULL";
        }

        public Builder feNodes(String feNodes) {
            config.feNodes = feNodes;
            return this;
        }

        public Builder username(String username) {
            config.username = username;
            return this;
        }

        public Builder password(String password) {
            config.password = password;
            return this;
        }

        public Builder database(String database) {
            config.database = database;
            return this;
        }

        public Builder table(String table) {
            config.table = table;
            return this;
        }

        public Builder batchSize(int batchSize) {
            config.batchSize = batchSize;
            return this;
        }

        public Builder batchIntervalMs(long batchIntervalMs) {
            config.batchIntervalMs = batchIntervalMs;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            config.maxRetries = maxRetries;
            return this;
        }

        public Builder partialUpdate(boolean partialUpdate) {
            config.partialUpdate = partialUpdate;
            return this;
        }

        public Builder mergeType(String mergeType) {
            config.mergeType = mergeType;
            return this;
        }

        public Builder format(String format) {
            config.format = format;
            return this;
        }

        public Builder maxFilterRatio(String maxFilterRatio) {
            config.maxFilterRatio = maxFilterRatio;
            return this;
        }

        public Builder timeout(String timeout) {
            config.timeout = timeout;
            return this;
        }

        public Builder strictMode(boolean strictMode) {
            config.strictMode = strictMode;
            return this;
        }

        public Builder loadMemLimit(String loadMemLimit) {
            config.loadMemLimit = loadMemLimit;
            return this;
        }

        public Builder labelPrefix(String labelPrefix) {
            config.labelPrefix = labelPrefix;
            return this;
        }

        public Builder serializerType(String serializerType) {
            config.serializerType = serializerType;
            return this;
        }

        public Builder includeStrategy(String includeStrategy) {
            config.includeStrategy = includeStrategy;
            return this;
        }

        public DorisSinkConfig build() {
            // 验证必填字段
            if (config.feNodes == null || config.feNodes.isEmpty()) {
                throw new IllegalArgumentException("feNodes must not be null or empty");
            }
            if (config.database == null || config.database.isEmpty()) {
                throw new IllegalArgumentException("database must not be null or empty");
            }
            if (config.table == null || config.table.isEmpty()) {
                throw new IllegalArgumentException("table must not be null or empty");
            }
            return config;
        }
    }

    // ========== Getters ==========

    public String getFeNodes() {
        return feNodes;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public boolean isPartialUpdate() {
        return partialUpdate;
    }

    public String getMergeType() {
        return mergeType;
    }

    public String getFormat() {
        return format;
    }

    public boolean isReadJsonByLine() {
        return readJsonByLine;
    }

    public boolean isStripOuterArray() {
        return stripOuterArray;
    }

    public String getMaxFilterRatio() {
        return maxFilterRatio;
    }

    public String getTimeout() {
        return timeout;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public String getLoadMemLimit() {
        return loadMemLimit;
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    public String getSerializerType() {
        return serializerType;
    }

    public String getIncludeStrategy() {
        return includeStrategy;
    }

    /**
     * 生成 Stream Load Properties
     */
    public Properties toStreamLoadProperties() {
        Properties props = new Properties();
        props.setProperty("format", format);
        props.setProperty("read_json_by_line", String.valueOf(readJsonByLine));
        props.setProperty("strip_outer_array", String.valueOf(stripOuterArray));
        props.setProperty("max_filter_ratio", maxFilterRatio);
        props.setProperty("timeout", timeout);
        props.setProperty("strict_mode", String.valueOf(strictMode));
        props.setProperty("load_mem_limit", loadMemLimit);

        // 部分列更新配置
        if (partialUpdate) {
            props.setProperty("partial_columns", "true");
            props.setProperty("merge_type", mergeType);
        }

        return props;
    }
}