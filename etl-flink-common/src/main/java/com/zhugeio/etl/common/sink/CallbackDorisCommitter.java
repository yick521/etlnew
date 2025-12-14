package com.zhugeio.etl.common.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisAbstractCommittable;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.committer.DorisCommitter;
import org.apache.doris.flink.sink.copy.DorisCopyCommittable;
import org.apache.doris.flink.sink.copy.DorisCopyCommitter;
import org.apache.flink.api.connector.sink2.Committer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 带回调的 DorisCommitter (适配 flink-doris-connector 25.1.0 + Flink 1.17.2)
 * 
 * 实现原理:
 * 1. 根据 committable 类型分发到对应的 committer (DorisCommitter / DorisCopyCommitter)
 * 2. commit 成功后触发回调，上报成功写入的记录数
 * 3. commit 失败抛异常，回调不执行
 */
public class CallbackDorisCommitter implements Committer<DorisAbstractCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(CallbackDorisCommitter.class);

    private final DorisCommitter streamLoadCommitter;
    private final DorisCopyCommitter copyCommitter;
    private final CommitSuccessCallback callback;
    private final String tableName;
    private final CountingSerializer<?> countingSerializer;

    /**
     * 构造函数
     */
    public CallbackDorisCommitter(DorisOptions dorisOptions,
                                   DorisReadOptions dorisReadOptions,
                                   DorisExecutionOptions executionOptions,
                                   CommitSuccessCallback callback,
                                   String tableName,
                                   CountingSerializer<?> countingSerializer) {
        // 创建 stream load committer
        this.streamLoadCommitter = new DorisCommitter(dorisOptions, dorisReadOptions, executionOptions);
        
        // 创建 copy committer (用于 copy into 模式)
        this.copyCommitter = new DorisCopyCommitter(dorisOptions, executionOptions.getMaxRetries());
        
        this.callback = callback;
        this.tableName = tableName;
        this.countingSerializer = countingSerializer;
    }

    @Override
    public void commit(Collection<CommitRequest<DorisAbstractCommittable>> requests)
            throws IOException, InterruptedException {

        // 记录 commit 前的计数
        long countBeforeCommit = countingSerializer != null ? countingSerializer.getAndResetCount() : 0;

        // 分离不同类型的 committable
        List<CommitRequest<DorisCommittable>> streamLoadRequests = new ArrayList<>();
        List<CommitRequest<DorisCopyCommittable>> copyRequests = new ArrayList<>();
        
        for (CommitRequest<DorisAbstractCommittable> request : requests) {
            DorisAbstractCommittable committable = request.getCommittable();
            if (committable instanceof DorisCommittable) {
                streamLoadRequests.add(new CommitRequestAdapter<>((DorisCommittable) committable, request));
            } else if (committable instanceof DorisCopyCommittable) {
                copyRequests.add(new CommitRequestAdapter<>((DorisCopyCommittable) committable, request));
            }
        }

        // 提交 stream load 事务
        if (!streamLoadRequests.isEmpty()) {
            streamLoadCommitter.commit(streamLoadRequests);
        }
        
        // 提交 copy into 事务
        if (!copyRequests.isEmpty()) {
            copyCommitter.commit(copyRequests);
        }

        // commit 成功，触发回调
        if (callback != null && countBeforeCommit > 0) {
            try {
                callback.onCommitSuccess(tableName, countBeforeCommit);
                LOG.debug("[CallbackDorisCommitter] Commit 成功回调: table={}, count={}",
                        tableName, countBeforeCommit);
            } catch (Exception e) {
                // 回调失败不影响主流程
                LOG.warn("[CallbackDorisCommitter] 回调执行失败: {}", e.getMessage());
            }
        }
    }

    @Override
    public void close() throws Exception {
        streamLoadCommitter.close();
        copyCommitter.close();
    }
    
    /**
     * CommitRequest 适配器，用于类型转换
     * 
     * 实现 Flink 1.17.2 CommitRequest 接口的所有 7 个方法
     */
    private static class CommitRequestAdapter<T> implements CommitRequest<T> {
        private final T committable;
        private final CommitRequest<?> originalRequest;
        
        CommitRequestAdapter(T committable, CommitRequest<?> originalRequest) {
            this.committable = committable;
            this.originalRequest = originalRequest;
        }
        
        @Override
        public T getCommittable() {
            return committable;
        }
        
        @Override
        public int getNumberOfRetries() {
            return originalRequest.getNumberOfRetries();
        }
        
        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            originalRequest.signalFailedWithKnownReason(t);
        }
        
        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            originalRequest.signalFailedWithUnknownReason(t);
        }
        
        @Override
        public void retryLater() {
            originalRequest.retryLater();
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public void updateAndRetryLater(T newCommittable) {
            ((CommitRequest<Object>) originalRequest).updateAndRetryLater(newCommittable);
        }
        
        @Override
        public void signalAlreadyCommitted() {
            originalRequest.signalAlreadyCommitted();
        }
    }
}
