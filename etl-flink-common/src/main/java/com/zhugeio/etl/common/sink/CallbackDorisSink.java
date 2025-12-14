package com.zhugeio.etl.common.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisAbstractCommittable;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.DorisAbstractWriter;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;

/**
 * 带回调的 DorisSink (适配 flink-doris-connector 25.1.0 + Flink 1.17.2)
 *
 * 在 2PC commit 成功后触发回调，用于统计成功写入 Doris 的记录数
 */
public class CallbackDorisSink<T> implements StatefulSink<T, DorisWriterState>,
        TwoPhaseCommittingSink<T, DorisAbstractCommittable> {

    private final DorisSink<T> delegateSink;
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions dorisExecutionOptions;
    private final CommitSuccessCallback callback;
    private final String tableName;
    private final CountingSerializer<T> countingSerializer;

    private CallbackDorisSink(DorisOptions dorisOptions,
                              DorisReadOptions dorisReadOptions,
                              DorisExecutionOptions dorisExecutionOptions,
                              CountingSerializer<T> countingSerializer,
                              CommitSuccessCallback callback,
                              String tableName) {
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.dorisExecutionOptions = dorisExecutionOptions;
        this.countingSerializer = countingSerializer;
        this.callback = callback;
        this.tableName = tableName;

        // 创建底层 DorisSink (使用计数序列化器)
        this.delegateSink = DorisSink.<T>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(dorisReadOptions)
                .setDorisExecutionOptions(dorisExecutionOptions)
                .setSerializer(countingSerializer)
                .build();
    }

    /**
     * 返回类型与 DorisSink.createWriter 一致: DorisAbstractWriter
     */
    @Override
    public DorisAbstractWriter createWriter(InitContext context)
            throws IOException {
        return delegateSink.createWriter(context);
    }

    /**
     * 返回类型与 DorisSink.restoreWriter 一致: DorisAbstractWriter
     */
    @Override
    public DorisAbstractWriter restoreWriter(InitContext context, Collection<DorisWriterState> recoveredState) throws IOException {
        return delegateSink.restoreWriter(context, recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<DorisWriterState> getWriterStateSerializer() {
        return delegateSink.getWriterStateSerializer();
    }

    @Override
    public Committer<DorisAbstractCommittable> createCommitter() throws IOException {
        return new CallbackDorisCommitter(
                dorisOptions,
                dorisReadOptions,
                dorisExecutionOptions,
                callback,
                tableName,
                countingSerializer
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public SimpleVersionedSerializer<DorisAbstractCommittable> getCommittableSerializer() {
        return (SimpleVersionedSerializer<DorisAbstractCommittable>)
                delegateSink.getCommittableSerializer();
    }

    // ==================== Builder ====================

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private DorisOptions dorisOptions;
        private DorisReadOptions dorisReadOptions;
        private DorisExecutionOptions dorisExecutionOptions;
        private DorisRecordSerializer<T> serializer;
        private CommitSuccessCallback callback;
        private String tableName;

        public Builder<T> setDorisOptions(DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public Builder<T> setDorisReadOptions(DorisReadOptions dorisReadOptions) {
            this.dorisReadOptions = dorisReadOptions;
            return this;
        }

        public Builder<T> setDorisExecutionOptions(DorisExecutionOptions dorisExecutionOptions) {
            this.dorisExecutionOptions = dorisExecutionOptions;
            return this;
        }

        public Builder<T> setSerializer(DorisRecordSerializer<T> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<T> setCommitCallback(CommitSuccessCallback callback) {
            this.callback = callback;
            return this;
        }

        public Builder<T> setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public CallbackDorisSink<T> build() {
            if (dorisOptions == null) {
                throw new IllegalArgumentException("DorisOptions is required");
            }
            if (dorisExecutionOptions == null) {
                throw new IllegalArgumentException("DorisExecutionOptions is required");
            }
            if (serializer == null) {
                throw new IllegalArgumentException("Serializer is required");
            }
            if (dorisReadOptions == null) {
                dorisReadOptions = DorisReadOptions.builder().build();
            }

            // 包装为计数序列化器
            CountingSerializer<T> countingSerializer = new CountingSerializer<>(serializer);

            return new CallbackDorisSink<>(
                    dorisOptions,
                    dorisReadOptions,
                    dorisExecutionOptions,
                    countingSerializer,
                    callback,
                    tableName
            );
        }
    }
}