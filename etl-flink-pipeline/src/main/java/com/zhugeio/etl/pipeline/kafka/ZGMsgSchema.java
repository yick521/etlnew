package com.zhugeio.etl.pipeline.kafka;

import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.UnsupportedEncodingException;

/**
 * @author ningjh
 * @name ZGMessageDeserializationSchema
 * @date 2025/12/1
 * @description
 */
public class ZGMsgSchema implements KafkaRecordDeserializationSchema<ZGMessage> {
    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<ZGMessage> out) {

        // 直接从 ConsumerRecord 中获取 Kafka 元数据
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        long timestamp = record.timestamp();

        // 反序列化消息体 (这里假设你的原始数据在 value 中)
        String rawData = null;
        try {
            rawData = new String(record.value(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        // 构造你的消息对象，并注入分区信息
        ZGMessage message = new ZGMessage();
        message.setTopic(topic);
        message.setPartition(partition);  // 关键：设置分区ID
        message.setOffset(offset);
        message.setRawData(rawData);

        out.collect(message);
    }

    @Override
    public TypeInformation<ZGMessage> getProducedType() {
        return TypeInformation.of(ZGMessage.class);
    }
}
