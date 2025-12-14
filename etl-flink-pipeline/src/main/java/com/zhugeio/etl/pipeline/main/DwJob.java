package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.pipeline.dataquality.DataQualityKafkaService;
import com.zhugeio.etl.common.model.output.DeviceRow;
import com.zhugeio.etl.common.model.output.EventAttrRow;
import com.zhugeio.etl.common.model.output.UserPropertyRow;
import com.zhugeio.etl.common.model.output.UserRow;
import com.zhugeio.etl.common.sink.*;
import com.zhugeio.etl.pipeline.config.Config;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.operator.dw.*;
import com.zhugeio.etl.pipeline.sink.DorisSinkFactory;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.zhugeio.etl.pipeline.config.Config.CHECKPOINT_DW_PATH;

/**
 * DW 层 ETL 主任务
 * 
 * 数据流程：
 * 1. Kafka Source → ZGMessage (已经过 Gate 处理，data 已解析)
 * 2. ZGMessage → IP富化 → UA富化 → 关键词富化
 * 3. 富化后的 ZGMessage → 数据路由（按 dt 类型分流）
 * 4. 各流 → 对应的 Doris 表
 * 
 * 输出表：
 * - b_user_{appid}           用户映射表
 * - b_device_{appid}         设备维度表
 * - b_user_property_{appid}  用户属性表
 * - b_user_event_attr_{appid} 事件属性表（主表）
 */
public class DwJob {

    private static final Logger LOG = LoggerFactory.getLogger(DwJob.class);

    public static void main(String[] args) throws Exception {

        LOG.info("╔════════════════════════════════════════╗");
        LOG.info("║     DW ETL Pipeline v2.0 启动中...     ║");
        LOG.info("╚════════════════════════════════════════╝");


        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置环境
        configureEnvironment(env);

        // 3. Kafka Source → ZGMessage
        DataStream<ZGMessage> sourceStream = buildKafkaSource(env);

        // 4. 数据富化流水线 (ZGMessage → ZGMessage)
        DataStream<ZGMessage> enrichedStream = buildEnrichmentPipeline(sourceStream);

        // 5. 数据路由（分流）
        SingleOutputStreamOperator<EventAttrRow> routedStream = enrichedStream
            .process(new DataRouterOperator())
            .name("DataRouterOperator")
            .uid("data-router-operator");

        // 6. 提取侧输出流
        DataStream<UserRow> userStream = routedStream
            .getSideOutput(DataRouterOperator.USER_OUTPUT);

        DataStream<DeviceRow> deviceStream = routedStream
            .getSideOutput(DataRouterOperator.DEVICE_OUTPUT);

        DataStream<UserPropertyRow> userPropertyStream = routedStream
            .getSideOutput(DataRouterOperator.USER_PROPERTY_OUTPUT);

        // 主输出就是 EventAttrRow
        DataStream<EventAttrRow> eventAttrStream = routedStream;

        // 7. 打印统计信息
        LOG.info("数据流构建完成:");
        LOG.info("  → UserRow        → b_user_{appid}");
        LOG.info("  → DeviceRow      → b_device_{appid}");
        LOG.info("  → UserPropertyRow → b_user_property_{appid}");
        LOG.info("  → EventAttrRow   → b_user_event_attr_{appid}");

        // 8. Sink 到 Doris (带数据质量成功计数)
        boolean dataQualityEnabled = Config.getBoolean(Config.DQ_ENABLED, true);
        if (dataQualityEnabled) {
            sinkToDorisWithDataQuality(userStream, deviceStream, userPropertyStream, eventAttrStream);
        } else {
            sinkToDoris(userStream, deviceStream, userPropertyStream, eventAttrStream);
        }
        
        // 临时: 打印输出
        userStream.print("USER");
        deviceStream.print("DEVICE");
        userPropertyStream.print("USER_PROPERTY");
        eventAttrStream.print("EVENT_ATTR");

        // 11. 执行
        LOG.info("╔════════════════════════════════════════╗");
        LOG.info("║     开始执行 DW ETL Pipeline...        ║");
        LOG.info("╚════════════════════════════════════════╝");

        env.execute("DW-ETL-Pipeline-v2");
    }

    /**
     * 配置执行环境
     */
    private static void configureEnvironment(StreamExecutionEnvironment env) {

        // Checkpoint
        long checkpointInterval = env.getCheckpointInterval();
        String checkpointPath = "hdfs:///user/flink/checkpoints/" + Config.getString(CHECKPOINT_DW_PATH);

        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointInterval / 2);
        env.getCheckpointConfig().setCheckpointTimeout(600000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        LOG.info("Checkpoint 间隔: {}ms, 路径: {}", checkpointInterval, checkpointPath);

        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        LOG.info("重启策略: 固定延迟重启(3次, 10秒间隔)");
    }

    /**
     * 构建 Kafka Source → ZGMessage
     */
    private static DataStream<ZGMessage> buildKafkaSource(StreamExecutionEnvironment env) {
        
        String kafkaServers = Config.getString(Config.KAFKA_BROKERS, "localhost:9092");
        String kafkaTopic = Config.getString(Config.KAFKA_DW_SOURCE_TOPIC, "dw_events");
        String kafkaGroupId = Config.getString(Config.KAFKA_DW_GROUP_ID, "dw-etl-group");

        LOG.info("Kafka 配置:");
        LOG.info("  Servers: {}", kafkaServers);
        LOG.info("  Topic: {}", kafkaTopic);
        LOG.info("  GroupId: {}", kafkaGroupId);

        KafkaSource<ZGMessage> kafkaSource = KafkaSource.<ZGMessage>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(kafkaTopic)
            .setGroupId(kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new ZGMsgSchema())
            .build();

        return env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "KafkaSource-ZGMessage"
        ).uid("kafka-source-zgmessage");
    }

    /**
     * 构建数据富化流水线 (ZGMessage → ZGMessage)
     */
    private static DataStream<ZGMessage> buildEnrichmentPipeline(DataStream<ZGMessage> input) {

        LOG.info("构建数据富化流水线...");

        // 1. IP 地理位置富化
        DataStream<ZGMessage> withIp = AsyncDataStream.unorderedWait(
            input,
            new IpEnrichOperator(
                Config.getString(Config.IP_DATABASE_PATH, "/data/ip/ip.dat"),
                Config.getBoolean(Config.IP_DATABASE_RELOAD_ENABLED, true),
                Config.getLong(Config.IP_DATABASE_RELOAD_INTERVAL_SECONDS, 300L),
                Config.getBoolean(Config.HDFS_HA_ENABLED, false)
            ),
            Config.getLong(Config.OPERATOR_IP_TIMEOUT_MS, 60000L),
            TimeUnit.MILLISECONDS,
            Config.getInt(Config.OPERATOR_IP_CAPACITY, 100)
        ).name("IpEnrichOperator").uid("ip-enrich");

        LOG.info("  ✓ IP 地理位置富化算子");

        // 2. User-Agent 解析富化
        DataStream<ZGMessage> withUa = AsyncDataStream.unorderedWait(
            withIp,
            new UserAgentEnrichOperator(
                Config.getInt(Config.OPERATOR_UA_CACHE_SIZE, 10000),
                Config.getLong(Config.OPERATOR_UA_CACHE_EXPIRE_MINUTES, 60L)
            ),
            Config.getLong(Config.OPERATOR_UA_TIMEOUT_MS, 10000L),
            TimeUnit.MILLISECONDS,
            Config.getInt(Config.OPERATOR_UA_CAPACITY, 100)
        ).name("UserAgentEnrichOperator").uid("ua-enrich");

        LOG.info("  ✓ User-Agent 解析富化算子");

        // 3. 搜索关键词富化
        DataStream<ZGMessage> enriched = AsyncDataStream.unorderedWait(
            withUa,
            new SearchKeywordEnrichOperator(
                Config.getString(Config.KVROCKS_HOST, "localhost"),
                Config.getInt(Config.KVROCKS_PORT, 6379),
                Config.getBoolean(Config.KVROCKS_CLUSTER, false),
                Config.getInt(Config.KVROCKS_LOCAL_CACHE_SIZE, 5000),
                Config.getLong(Config.KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES, 30L)
            ),
            Config.getLong(Config.OPERATOR_KEYWORD_TIMEOUT_MS, 5000L),
            TimeUnit.MILLISECONDS,
            Config.getInt(Config.OPERATOR_KEYWORD_CAPACITY, 100)
        ).name("SearchKeywordEnrichOperator").uid("search-keyword-enrich");

        LOG.info("  ✓ 搜索关键词富化算子");
        LOG.info("富化流水线构建完成");

        return enriched;
    }

    /**
     * Sink 到 Doris (带数据质量成功计数)
     *
     * 对应 Scala: MsgWriter.write + ErrorDistributeService.writeSuccessCount2SSDB
     */
    private static void sinkToDorisWithDataQuality(
            DataStream<UserRow> userStream,
            DataStream<DeviceRow> deviceStream,
            DataStream<UserPropertyRow> userPropertyStream,
            DataStream<EventAttrRow> eventAttrStream) {

        LOG.info("配置 Doris Sink (带数据质量统计)...");

        String feNodes = Config.getString(Config.DORIS_FE_NODES, "localhost:8030");
        String username = Config.getString(Config.DORIS_USERNAME, "root");
        String password = Config.getString(Config.DORIS_PASSWORD, "");
        String database = Config.getString(Config.DORIS_DATABASE, "dwd");

        LOG.info("Doris 配置:");
        LOG.info("  FE节点: {}", feNodes);
        LOG.info("  用户名: {}", username);
        LOG.info("  数据库: {}", database);

        DataQualityKafkaService dqService = DataQualityKafkaService.getInstance();

        // ============ 用户表 (b_user) ============
        DorisSinkConfig userConfig = DorisSinkFactory.partialUpdateConfig(
                feNodes, database, "b_user", username, password);

        CallbackDorisSink<UserRow> userSink = DorisSinkFactory.createWithCallback(
                userConfig,
                JsonSerializerFactory.createDefaultSerializer(),
                database + ".b_user",
                createSuccessCallback(dqService)
        );
        userStream.sinkTo(userSink).name("DorisSink-User").uid("doris-sink-user");
        LOG.info("  ✓ 用户表 Sink (b_user) - 带数据质量统计");

        // ============ 设备表 (b_device) ============
        DorisSinkConfig deviceConfig = DorisSinkFactory.partialUpdateConfig(
                feNodes, database, "b_device", username, password);

        CallbackDorisSink<DeviceRow> deviceSink = DorisSinkFactory.createWithCallback(
                deviceConfig,
                JsonSerializerFactory.createDefaultSerializer(),
                database + ".b_device",
                createSuccessCallback(dqService)
        );
        deviceStream.sinkTo(deviceSink).name("DorisSink-Device").uid("doris-sink-device");
        LOG.info("  ✓ 设备表 Sink (b_device) - 带数据质量统计");

        // ============ 用户属性表 (b_user_property) ============
        DorisSinkConfig userPropConfig = DorisSinkFactory.partialUpdateConfig(
                feNodes, database, "b_user_property", username, password);

        CallbackDorisSink<UserPropertyRow> userPropSink = DorisSinkFactory.createWithCallback(
                userPropConfig,
                JsonSerializerFactory.createDefaultSerializer(),
                database + ".b_user_property",
                createSuccessCallback(dqService)
        );
        userPropertyStream.sinkTo(userPropSink).name("DorisSink-UserProperty").uid("doris-sink-user-property");
        LOG.info("  ✓ 用户属性表 Sink (b_user_property) - 带数据质量统计");

        // ============ 事件属性表 (b_user_event_attr) - 高吞吐 ============
        DorisSinkConfig eventConfig = DorisSinkFactory.highThroughputConfig(
                feNodes, database, "b_user_event_attr", username, password);

        CallbackDorisSink<EventAttrRow> eventSink = DorisSinkFactory.createWithCallback(
                eventConfig,
                JsonSerializerFactory.createDefaultSerializer(),
                database + ".b_user_event_attr",
                createSuccessCallback(dqService)
        );
        eventAttrStream.sinkTo(eventSink).name("DorisSink-EventAttr").uid("doris-sink-event-attr");
        LOG.info("  ✓ 事件属性表 Sink (b_user_event_attr) - 带数据质量统计");

        LOG.info("Doris Sink 配置完成 (已集成数据质量服务)");
    }

    /**
     * 创建成功计数回调
     *
     * 对应 Scala: ErrorDistributeService.writeSuccessCount2SSDB
     *
     * 流程:
     * 1. Commit 成功后触发回调
     * 2. 记录成功计数到 dqService (key: success_count#tableName:day)
     * 3. 立即刷新成功计数到 Kafka (type: data-count)
     */
    private static CommitSuccessCallback createSuccessCallback(DataQualityKafkaService dqService) {
        return (tableName, count) -> {
            if (dqService != null && dqService.isEnabled()) {
                // 记录成功计数
                dqService.recordSuccessCount(tableName, count);
                // 立即刷新到 Kafka (与 Scala 行为一致: 每次 write 后立即发送)
                dqService.flushSuccessCounts();
            }
        };
    }

    /**
     * Sink 到 Doris (不带数据质量)
     */
    private static void sinkToDoris(
            DataStream<UserRow> userStream,
            DataStream<DeviceRow> deviceStream,
            DataStream<UserPropertyRow> userPropertyStream,
            DataStream<EventAttrRow> eventAttrStream) {

        LOG.info("配置 Doris Sink...");

        String feNodes = Config.getString(Config.DORIS_FE_NODES, "localhost:8030");
        String username = Config.getString(Config.DORIS_USERNAME, "root");
        String password = Config.getString(Config.DORIS_PASSWORD, "");
        String database = Config.getString(Config.DORIS_DATABASE, "dwd");

        LOG.info("Doris 配置:");
        LOG.info("  FE节点: {}", feNodes);
        LOG.info("  用户名: {}", username);
        LOG.info("  数据库: {}", database);

        // 用户表 (部分列更新)
        DorisSinkConfig userConfig = DorisSinkFactory.partialUpdateConfig(
                feNodes, database, "b_user", username, password);
        DorisSink<UserRow> userSink = DorisSinkFactory.create(userConfig);
        userStream.sinkTo(userSink).name("DorisSink-User").uid("doris-sink-user");
        LOG.info("  ✓ 用户表 Sink (b_user)");

        // 设备表 (部分列更新)
        DorisSinkConfig deviceConfig = DorisSinkFactory.partialUpdateConfig(
                feNodes, database, "b_device", username, password);
        DorisSink<DeviceRow> deviceSink = DorisSinkFactory.create(deviceConfig);
        deviceStream.sinkTo(deviceSink).name("DorisSink-Device").uid("doris-sink-device");
        LOG.info("  ✓ 设备表 Sink (b_device)");

        // 用户属性表 (部分列更新)
        DorisSinkConfig userPropConfig = DorisSinkFactory.partialUpdateConfig(
                feNodes, database, "b_user_property", username, password);
        DorisSink<UserPropertyRow> userPropSink = DorisSinkFactory.create(userPropConfig);
        userPropertyStream.sinkTo(userPropSink).name("DorisSink-UserProperty").uid("doris-sink-user-property");
        LOG.info("  ✓ 用户属性表 Sink (b_user_property)");

        // 事件属性表 (高吞吐)
        DorisSinkConfig eventConfig = DorisSinkFactory.highThroughputConfig(
                feNodes, database, "b_user_event_attr", username, password);
        DorisSink<EventAttrRow> eventSink = DorisSinkFactory.create(eventConfig);
        eventAttrStream.sinkTo(eventSink).name("DorisSink-EventAttr").uid("doris-sink-event-attr");
        LOG.info("  ✓ 事件属性表 Sink (b_user_event_attr)");

        LOG.info("Doris Sink 配置完成");
    }
}
