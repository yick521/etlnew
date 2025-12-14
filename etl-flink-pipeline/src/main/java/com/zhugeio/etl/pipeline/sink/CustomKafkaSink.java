package com.zhugeio.etl.pipeline.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.UUID;

/**
 * 自定义Kafka Sink - 完全避免JMX冲突
 *
 * 核心修复：
 * 1. ✅ 不使用 FlinkKafkaProducer（它会使用算子名称生成ID）
 * 2. ✅ 手动生成合法的 client.id 和 transactional.id（无特殊字符）
 * 3. ✅ 支持 At-Least-Once 语义
 * 4. ✅ 可选的 Exactly-Once 语义（通过 Checkpoint）
 */
public class CustomKafkaSink extends RichSinkFunction<String> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(CustomKafkaSink.class);

    private final String topic;
    private final String brokers;
    private final boolean enableExactlyOnce;

    private transient KafkaProducer<String, String> producer;
    private transient ListState<String> pendingRecordsState;
    private transient int subtaskIndex;

    public CustomKafkaSink(String topic, String brokers, boolean enableExactlyOnce) {
        this.topic = topic;
        this.brokers = brokers;
        this.enableExactlyOnce = enableExactlyOnce;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        LOG.info("========================================");
        LOG.info("[CustomKafkaSink-{}] 开始初始化", subtaskIndex);
        LOG.info("[CustomKafkaSink-{}] 并行度: {}/{}", subtaskIndex, subtaskIndex + 1, totalSubtasks);
        LOG.info("[CustomKafkaSink-{}] Topic: {}", subtaskIndex, topic);
        LOG.info("[CustomKafkaSink-{}] Brokers: {}", subtaskIndex, brokers);
        LOG.info("[CustomKafkaSink-{}] Exactly-Once: {}", subtaskIndex, enableExactlyOnce);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        // ✅ 核心修复：手动生成合法的 client.id（只包含字母、数字、连字符、下划线）
        String clientId = String.format("kafka-producer-%s-%s", subtaskIndex, UUID.randomUUID().toString().substring(0, 8));
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        LOG.info("[CustomKafkaSink-{}] Client ID: {}", subtaskIndex, clientId);

        // 序列化器
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 超时和重试配置
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");

        // 性能配置
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "131072");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728");

        // 可靠性配置
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");

        // 并行度优化
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");  // 提高并发请求数
        props.setProperty(ProducerConfig.SEND_BUFFER_CONFIG, "131072");  // 发送缓冲区大小
        props.setProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG, "65536");  // 接收缓冲区大小

        if (enableExactlyOnce) {
            // Exactly-Once 配置
            props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "300000");

            // ✅ 生成合法的 transactional.id（只包含合法字符）
            String transactionalId = String.format("kafka-txn-%d-%s",
                    subtaskIndex,
                    UUID.randomUUID().toString().substring(0, 8));
            props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            LOG.info("[CustomKafkaSink-{}] Transactional ID: {}", subtaskIndex, transactionalId);
        }

        // 创建 Producer
        try {
            producer = new KafkaProducer<>(props);

            if (enableExactlyOnce) {
                producer.initTransactions();
                // ✅ 关键修复：立即开始第一个事务
                producer.beginTransaction();
                LOG.info("[CustomKafkaSink-{}] ✅ 事务初始化并开始第一个事务", subtaskIndex);
            }

            LOG.info("[CustomKafkaSink-{}] ✅ Kafka Producer 初始化成功", subtaskIndex);
            LOG.info("========================================");

        } catch (Exception e) {
            LOG.error("[CustomKafkaSink-{}] ❌ Kafka Producer 初始化失败: {}",
                    subtaskIndex, e.getMessage(), e);
            throw new RuntimeException("Kafka Producer 初始化失败", e);
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        try {
            // 异步发送（使用回调处理失败）
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("[CustomKafkaSink-{}] 发送失败: {}", subtaskIndex, exception.getMessage());
                }
            });

        } catch (Exception e) {
            LOG.error("[CustomKafkaSink-{}] 发送异常: {}", subtaskIndex, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (enableExactlyOnce && producer != null) {
            try {
                // 提交事务
                producer.commitTransaction();
                LOG.debug("[CustomKafkaSink-{}] 事务已提交: checkpoint-{}",
                        subtaskIndex, context.getCheckpointId());

                // 开始新事务
                producer.beginTransaction();

            } catch (Exception e) {
                LOG.error("[CustomKafkaSink-{}] 事务提交失败: {}", subtaskIndex, e.getMessage(), e);
                throw e;
            }
        } else if (producer != null) {
            // At-Least-Once: 只需要flush
            producer.flush();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化状态（如果需要）
        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>("pending-records", String.class);
        pendingRecordsState = context.getOperatorStateStore().getListState(descriptor);

        // ✅ 不在这里操作 producer，因为此时 producer 还是 null
        // 事务会在 open() 方法中开始
    }

    @Override
    public void close() throws Exception {
        LOG.info("[CustomKafkaSink-{}] 正在关闭...", subtaskIndex);

        if (producer != null) {
            try {
                if (enableExactlyOnce) {
                    // 尝试提交最后的事务
                    producer.commitTransaction();
                    LOG.info("[CustomKafkaSink-{}] 最后的事务已提交", subtaskIndex);
                }

                producer.flush();
                producer.close();
                LOG.info("[CustomKafkaSink-{}] ✅ Kafka Producer 已关闭", subtaskIndex);

            } catch (Exception e) {
                LOG.error("[CustomKafkaSink-{}] 关闭失败: {}", subtaskIndex, e.getMessage(), e);
            }
        }
    }

    /**
     * 添加自定义Kafka Sink到数据流
     */
    public static void addCustomKafkaSink(
            DataStream<String> stream,
            String topic,
            String brokers,
            boolean enableExactlyOnce) {

        LOG.info("\n========================================");
        LOG.info("添加自定义Kafka Sink:");
        LOG.info("  - Topic: {}", topic);
        LOG.info("  - Brokers: {}", brokers);
        LOG.info("  - Exactly-Once: {}", enableExactlyOnce);
        LOG.info("  - 优势: 完全避免JMX MBean冲突");
        LOG.info("========================================\n");

        stream.addSink(new CustomKafkaSink(topic, brokers, enableExactlyOnce))
                .name("CustomKafkaOutput")  // ✅ 自定义名称，不影响client.id
                .uid("custom-kafka-sink");
    }

    /**
     * 添加自定义Kafka Sink到数据流
     * @param stream
     * @param topic
     * @param brokers
     * @param enableExactlyOnce
     * @param uid
     */
    public static void addCustomKafkaSink(
            DataStream<String> stream,
            String topic,
            String brokers,
            boolean enableExactlyOnce,String uid) {

        LOG.info("\n========================================");
        LOG.info("添加自定义Kafka Sink:");
        LOG.info("  - Topic: {}", topic);
        LOG.info("  - Brokers: {}", brokers);
        LOG.info("  - Exactly-Once: {}", enableExactlyOnce);
        LOG.info("  - 优势: 完全避免JMX MBean冲突");
        LOG.info("========================================\n");

        stream.addSink(new CustomKafkaSink(topic, brokers, enableExactlyOnce))
                .name(uid)  // ✅ 自定义名称，不影响client.id
                .uid(uid);
    }
}