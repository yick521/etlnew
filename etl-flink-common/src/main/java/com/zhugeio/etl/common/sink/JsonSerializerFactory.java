package com.zhugeio.etl.common.sink;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * JSON 序列化器工厂
 */
public class JsonSerializerFactory {

    /**
     * 创建 Jackson 序列化器
     */
    public static <T> DorisRecordSerializer<T> createJacksonSerializer(String includeStrategy) {
        JsonInclude.Include include = parseIncludeStrategy(includeStrategy);
        return new JacksonSerializer<>(include);
    }

    /**
     * 创建默认序列化器 (过滤null)
     */
    public static <T> DorisRecordSerializer<T> createDefaultSerializer() {
        return new JacksonSerializer<>(JsonInclude.Include.NON_NULL);
    }

    /**
     * 解析包含策略
     */
    private static JsonInclude.Include parseIncludeStrategy(String strategy) {
        if (strategy == null) {
            return JsonInclude.Include.NON_NULL;
        }

        switch (strategy.toUpperCase()) {
            case "NON_NULL":
                return JsonInclude.Include.NON_NULL;
            case "NON_EMPTY":
                return JsonInclude.Include.NON_EMPTY;
            case "NON_DEFAULT":
                return JsonInclude.Include.NON_DEFAULT;
            case "ALWAYS":
                return JsonInclude.Include.ALWAYS;
            default:
                return JsonInclude.Include.NON_NULL;
        }
    }

    /**
     * Jackson 序列化器实现
     */
    private static class JacksonSerializer<T> implements DorisRecordSerializer<T> {

        private static final long serialVersionUID = 1L;

        private transient ObjectMapper objectMapper;
        private final JsonInclude.Include includeStrategy;

        public JacksonSerializer(JsonInclude.Include includeStrategy) {
            this.includeStrategy = includeStrategy;
        }

        private void initObjectMapper() {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
                objectMapper.setSerializationInclusion(includeStrategy);
            }
        }

        @Override
        public DorisRecord serialize(T record) throws IOException {
            initObjectMapper();
            String json = objectMapper.writeValueAsString(record);
            return DorisRecord.of(json.getBytes(StandardCharsets.UTF_8));
        }
    }
}
