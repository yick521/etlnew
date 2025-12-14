package com.zhugeio.etl.common.sink;

import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带计数功能的 DorisRecordSerializer 包装器
 * 
 * 包装原始序列化器，统计序列化成功的记录数
 * 
 * 用法:
 * 1. 包装原始序列化器: new CountingSerializer<>(originalSerializer)
 * 2. 在需要时获取并重置计数: long count = countingSerializer.getAndResetCount()
 */
public class CountingSerializer<T> implements DorisRecordSerializer<T> {
    
    private final DorisRecordSerializer<T> delegate;
    private final AtomicLong counter = new AtomicLong(0);
    private CountListener listener;
    
    /**
     * 计数监听器 (可选)
     */
    public interface CountListener {
        void onCount(long currentCount);
    }
    
    public CountingSerializer(DorisRecordSerializer<T> delegate) {
        this.delegate = delegate;
    }
    
    public void setCountListener(CountListener listener) {
        this.listener = listener;
    }
    
    @Override
    public DorisRecord serialize(T record) throws IOException {
        DorisRecord result = delegate.serialize(record);
        if (result != null) {
            long current = counter.incrementAndGet();
            if (listener != null) {
                listener.onCount(current);
            }
        }
        return result;
    }
    
    @Override
    public void initial() {
        delegate.initial();
    }
    
    @Override
    public DorisRecord flush() {
        return delegate.flush();
    }
    
    /**
     * 获取当前计数并重置为0
     * 
     * @return 重置前的计数值
     */
    public long getAndResetCount() {
        return counter.getAndSet(0);
    }
    
    /**
     * 获取当前计数 (不重置)
     */
    public long getCount() {
        return counter.get();
    }
    
    /**
     * 重置计数为0
     */
    public void resetCount() {
        counter.set(0);
    }
    
    /**
     * 获取原始序列化器
     */
    public DorisRecordSerializer<T> getDelegate() {
        return delegate;
    }
}
