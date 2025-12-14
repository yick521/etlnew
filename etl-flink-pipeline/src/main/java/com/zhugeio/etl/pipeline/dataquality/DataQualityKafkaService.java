package com.zhugeio.etl.pipeline.dataquality;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.zhugeio.etl.pipeline.config.Config;

/**
 * 数据质量 Kafka 服务
 * 
 * 统一管理:
 * 1. 错误日志发送 (error-log) - 由 Operator 调用
 * 2. 成功计数发送 (data-count) - 由 Sink Commit 回调调用
 * 3. 错误计数发送 (data-count) - 由 Operator 调用
 * 
 * 消息格式与 Scala 完全一致
 */
public class DataQualityKafkaService implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DataQualityKafkaService.class);

    // 消息类型常量 (与 Scala 保持一致)
    public static final String DATA_COUNT_TYPE = "data-count";
    public static final String ERROR_LOG_TYPE = "error-log";
    public static final String SUCCESS_COUNT_KEY = "success_count";
    public static final String ERROR_COUNT_KEY = "error_count";

    private static volatile DataQualityKafkaService instance;

    private String kafkaBrokers;
    private String kafkaTopic;
    private boolean enabled;

    private transient KafkaProducer<String, String> producer;

    // 错误计数缓存 (Operator 阶段): key = "error_count#appId:day:plat:eventName"
    private transient ConcurrentHashMap<String, Long> errorCountCache;

    // 成功计数缓存 (Commit 回调): key = "success_count#tableName:day"
    private transient ConcurrentHashMap<String, Long> successCountCache;

    private static final ThreadLocal<SimpleDateFormat> DAY_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd"));

    private DataQualityKafkaService() {}

    public static DataQualityKafkaService getInstance() {
        if (instance == null) {
            synchronized (DataQualityKafkaService.class) {
                if (instance == null) {
                    instance = new DataQualityKafkaService();
                    instance.init(Config.getString(Config.DQ_KAFKA_BROKERS),
                            Config.getString(Config.DQ_KAFKA_TOPIC),
                            Config.getBoolean(Config.DQ_ENABLED, true));
                }
            }
        }
        return instance;
    }

    /**
     * 初始化服务
     */
    public void init(String kafkaBrokers, String kafkaTopic, boolean enabled) {
        this.kafkaBrokers = kafkaBrokers;
        this.kafkaTopic = kafkaTopic;
        this.enabled = enabled;

        if (!enabled) {
            LOG.info("[DataQualityKafkaService] 已禁用");
            return;
        }

        errorCountCache = new ConcurrentHashMap<>();
        successCountCache = new ConcurrentHashMap<>();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");

        producer = new KafkaProducer<>(props);
        LOG.info("[DataQualityKafkaService] 初始化成功, brokers={}, topic={}", kafkaBrokers, kafkaTopic);
    }

    // ==================== 错误日志 (Operator 阶段调用) ====================

    /**
     * 发送错误日志
     * 
     * 对应 Scala: ErrorDistributeService.distributeErrorMsg
     */
    public void sendErrorLog(ErrorMessageEnum errorEnum,
                             Integer appId,
                             Integer platform,
                             String eventName,
                             String dataJson,
                             Long ct,
                             String pl,
                             String sdk) {
        if (!enabled || producer == null) {
            return;
        }

        try {
            String currentDay = DAY_FORMAT.get().format(new Date());

            // 构建错误日志消息 (与 Scala 格式一致)
            JSONObject errorMsgJson = new JSONObject();
            JSONObject dataJsonObj = new JSONObject();

            dataJsonObj.put("app_id", appId);
            dataJsonObj.put("error_code", errorEnum.getErrorCode());
            dataJsonObj.put("data_json", dataJson);
            dataJsonObj.put("data_md5", getMD5(dataJson));
            dataJsonObj.put("error_md5", getMD5(errorEnum.getErrorMessage()));
            dataJsonObj.put("log_utc_date", System.currentTimeMillis());
            dataJsonObj.put("log_utc_day_id", currentDay);
            dataJsonObj.put("event_begin_date", ct != null ? String.valueOf(ct) : "0");
            dataJsonObj.put("pl", pl != null ? pl : "");
            dataJsonObj.put("sdk", sdk != null ? sdk : "");
            dataJsonObj.put("platform", platform);
            dataJsonObj.put("pro_flag", 0);
            dataJsonObj.put("event_name", eventName != null ? eventName : "\\N");
            dataJsonObj.put("error_msg", errorEnum.getErrorMessage());

            errorMsgJson.put("data", dataJsonObj);
            errorMsgJson.put("type", ERROR_LOG_TYPE);

            // 发送错误日志
            sendToKafka(String.valueOf(appId), errorMsgJson.toJSONString());

            // 同时记录错误计数
            recordErrorCount(appId, platform, eventName);

            LOG.debug("[DataQuality] 发送错误日志: appId={}, errorCode={}",
                    appId, errorEnum.getErrorCode());

        } catch (Exception e) {
            LOG.warn("[DataQuality] 发送错误日志失败: {}", e.getMessage());
        }
    }

    /**
     * 记录错误计数 (累加到缓存)
     */
    public void recordErrorCount(Integer appId, Integer platform, String eventName) {
        if (!enabled || errorCountCache == null) {
            return;
        }

        String currentDay = DAY_FORMAT.get().format(new Date());
        String key = ERROR_COUNT_KEY + "#" + appId + ":" + currentDay + ":" + platform + ":" +
                (eventName != null ? eventName : "\\N");
        errorCountCache.merge(key, 1L, Long::sum);
    }

    // ==================== 成功计数 (Commit 回调调用) ====================

    /**
     * 记录入库成功计数
     * 
     * 由 Sink Commit 成功后调用
     * 
     * @param tableName 表名 (如 b_user_event_attr_123)
     * @param count 成功记录数
     */
    public void recordSuccessCount(String tableName, long count) {
        if (!enabled || successCountCache == null || count <= 0) {
            return;
        }

        String currentDay = DAY_FORMAT.get().format(new Date());
        // key 格式: success_count#tableName:day
        String key = SUCCESS_COUNT_KEY + "#" + tableName + ":" + currentDay;
        successCountCache.merge(key, count, Long::sum);

        LOG.debug("[DataQuality] 记录成功计数: table={}, count={}", tableName, count);
    }

    /**
     * 记录入库成功计数 (带详细维度)
     * 
     * @param appId 应用ID
     * @param platform 平台
     * @param eventName 事件名
     * @param count 成功记录数
     */
    public void recordSuccessCount(Integer appId, Integer platform, String eventName, long count) {
        if (!enabled || successCountCache == null || count <= 0) {
            return;
        }
        if (eventName == null || "\\N".equals(eventName)) {
            return;
        }

        String currentDay = DAY_FORMAT.get().format(new Date());
        // key 格式与 Scala 一致: success_count#appId:day:plat:eventName
        String key = SUCCESS_COUNT_KEY + "#" + appId + ":" + currentDay + ":" + platform + ":" + eventName;
        successCountCache.merge(key, count, Long::sum);
    }

    // ==================== 刷新计数到 Kafka ====================

    /**
     * 刷新所有计数到 Kafka
     * 
     * 对应 Scala: DistributeService.sendCount
     */
    public void flushCounts() {
        if (!enabled || producer == null) {
            return;
        }

        try {
            JSONObject countJson = new JSONObject();

            // 成功计数
            if (successCountCache != null && !successCountCache.isEmpty()) {
                Map<String, Long> snapshot = new HashMap<>(successCountCache);
                successCountCache.clear();
                for (Map.Entry<String, Long> entry : snapshot.entrySet()) {
                    countJson.put(entry.getKey(), entry.getValue());
                }
            }

            // 错误计数
            if (errorCountCache != null && !errorCountCache.isEmpty()) {
                Map<String, Long> snapshot = new HashMap<>(errorCountCache);
                errorCountCache.clear();
                for (Map.Entry<String, Long> entry : snapshot.entrySet()) {
                    countJson.put(entry.getKey(), entry.getValue());
                }
            }

            // 发送
            if (!countJson.isEmpty()) {
                JSONObject allCountJson = new JSONObject();
                allCountJson.put("data", countJson);
                allCountJson.put("type", DATA_COUNT_TYPE);

                sendToKafka(null, allCountJson.toJSONString());
                LOG.info("[DataQuality] 刷新计数: {} 条, 内容: {}", countJson.size(), allCountJson.toJSONString());
            }

        } catch (Exception e) {
            LOG.warn("[DataQuality] 刷新计数失败: {}", e.getMessage());
        }
    }

    /**
     * 只刷新成功计数 (Commit 回调后调用)
     */
    public void flushSuccessCounts() {
        if (!enabled || producer == null) {
            return;
        }

        try {
            if (successCountCache != null && !successCountCache.isEmpty()) {
                JSONObject countJson = new JSONObject();
                Map<String, Long> snapshot = new HashMap<>(successCountCache);
                successCountCache.clear();

                for (Map.Entry<String, Long> entry : snapshot.entrySet()) {
                    countJson.put(entry.getKey(), entry.getValue());
                }

                if (!countJson.isEmpty()) {
                    JSONObject allCountJson = new JSONObject();
                    allCountJson.put("data", countJson);
                    allCountJson.put("type", DATA_COUNT_TYPE);

                    sendToKafka(null, allCountJson.toJSONString());
                    LOG.info("[DataQuality] 刷新成功计数: {}", allCountJson.toJSONString());
                }
            }
        } catch (Exception e) {
            LOG.warn("[DataQuality] 刷新成功计数失败: {}", e.getMessage());
        }
    }

    // ==================== 工具方法 ====================

    private void sendToKafka(String key, String message) {
        if (producer != null) {
            producer.send(new ProducerRecord<>(kafkaTopic, key, message), (metadata, exception) -> {
                if (exception != null) {
                    LOG.warn("[DataQuality] Kafka 发送失败: {}", exception.getMessage());
                }
            });
        }
    }

    private String getMD5(String str) {
        if (str == null) {
            return "";
        }
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            md5.update(bytes, 0, bytes.length);
            return new BigInteger(1, md5.digest()).toString(16).toLowerCase();
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 关闭服务
     */
    public void close() {
        if (!enabled) {
            return;
        }

        // 关闭前刷新
        flushCounts();

        if (producer != null) {
            try {
                producer.flush();
                producer.close();
                LOG.info("[DataQualityKafkaService] 已关闭");
            } catch (Exception e) {
                LOG.warn("[DataQualityKafkaService] 关闭失败: {}", e.getMessage());
            }
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }
}
