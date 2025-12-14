package com.zhugeio.etl.pipeline.sink;

import com.zhugeio.etl.common.sink.CallbackDorisSink;
import com.zhugeio.etl.common.sink.CommitSuccessCallback;
import com.zhugeio.etl.common.sink.DorisSinkConfig;
import com.zhugeio.etl.pipeline.dataquality.DataQualityKafkaService;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;

/**
 * Doris Sink 工厂
 *
 * 提供多种创建 DorisSink 的方法:
 * 1. create() - 标准 DorisSink (无回调)
 * 2. createWithCallback() - 带 Commit 成功回调的 Sink
 * 3. createWithDataQuality() - 集成数据质量服务的 Sink
 */
public class DorisSinkFactory {

    // ==================== 标准创建方法 (无回调) ====================

    /**
     * 创建标准 DorisSink
     */
    public static <T> DorisSink<T> create(DorisSinkConfig config) {
        return DorisSink.<T>builder()
                .setDorisOptions(buildDorisOptions(config))
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(buildExecutionOptions(config))
                .build();
    }

    // ==================== 带回调的创建方法 ====================

    /**
     * 创建带 Commit 成功回调的 DorisSink
     *
     * @param config Doris 配置
     * @param serializer 序列化器
     * @param tableName 表名
     * @param callback Commit 成功回调
     */
    public static <T> CallbackDorisSink<T> createWithCallback(
            DorisSinkConfig config,
            DorisRecordSerializer<T> serializer,
            String tableName,
            CommitSuccessCallback callback) {

        return CallbackDorisSink.<T>builder()
                .setDorisOptions(buildDorisOptions(config))
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(buildExecutionOptions(config))
                .setSerializer(serializer)
                .setTableName(tableName)
                .setCommitCallback(callback)
                .build();
    }

    /**
     * 创建集成数据质量服务的 DorisSink
     *
     * Commit 成功后自动记录到 DataQualityKafkaService
     */
    public static <T> CallbackDorisSink<T> createWithDataQuality(
            DorisSinkConfig config,
            DorisRecordSerializer<T> serializer,
            String tableName,
            DataQualityKafkaService dqService) {

        CommitSuccessCallback callback = (table, count) -> {
            if (dqService != null && dqService.isEnabled()) {
                dqService.recordSuccessCount(table, count);
            }
        };

        return createWithCallback(config, serializer, tableName, callback);
    }

    // ==================== 内部构建方法 ====================

    /**
     * 根据 DorisSinkConfig 构建 DorisOptions
     */
    private static DorisOptions buildDorisOptions(DorisSinkConfig config) {
        return DorisOptions.builder()
                .setFenodes(config.getFeNodes())
                .setTableIdentifier(config.getDatabase() + "." + config.getTable())
                .setUsername(config.getUsername())
                .setPassword(config.getPassword())
                .build();
    }

    /**
     * 根据 DorisSinkConfig 构建 DorisExecutionOptions
     */
    private static DorisExecutionOptions buildExecutionOptions(DorisSinkConfig config) {
        return DorisExecutionOptions.builder()
                .setLabelPrefix(config.getLabelPrefix())
                .setBufferCount(config.getBatchSize())
                .setBufferFlushIntervalMs(config.getBatchIntervalMs())
                .setMaxRetries(config.getMaxRetries())
                .setStreamLoadProp(config.toStreamLoadProperties())
                .enable2PC()
                .build();
    }

    // ==================== 配置模板 ====================

    /**
     * 高吞吐配置 (适合大批量写入)
     */
    public static DorisSinkConfig highThroughputConfig(String feNodes, String db, String table,
                                                       String user, String password) {
        return DorisSinkConfig.builder()
                .feNodes(feNodes)
                .database(db)
                .table(table)
                .username(user)
                .password(password)
                .batchSize(50000)
                .batchIntervalMs(10000)
                .maxRetries(3)
                .format("json")
                .build();
    }

    /**
     * 低延迟配置 (适合实时写入)
     */
    public static DorisSinkConfig lowLatencyConfig(String feNodes, String db, String table,
                                                   String user, String password) {
        return DorisSinkConfig.builder()
                .feNodes(feNodes)
                .database(db)
                .table(table)
                .username(user)
                .password(password)
                .batchSize(5000)
                .batchIntervalMs(2000)
                .maxRetries(3)
                .format("json")
                .build();
    }

    /**
     * 部分列更新配置
     */
    public static DorisSinkConfig partialUpdateConfig(String feNodes, String db, String table,
                                                      String user, String password) {
        return DorisSinkConfig.builder()
                .feNodes(feNodes)
                .database(db)
                .table(table)
                .username(user)
                .password(password)
                .batchSize(10000)
                .batchIntervalMs(5000)
                .maxRetries(3)
                .format("json")
                .partialUpdate(true)
                .build();
    }
}