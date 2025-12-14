package com.zhugeio.etl.pipeline.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事件属性列映射服务
 * 
 * 业务逻辑:
 * - 根据 eventId + attrId 获取对应的 cus 列索引
 * - 使用两级缓存: L1 Caffeine (本地) → L2 KVRocks (分布式)
 * 
 * KVRocks Key 格式: ea:{eventId}_{attrId}
 * Value 格式: cus1 / cus2 / ... / cus100
 * 
 * 对应 Scala: FrontCache.eventAttrColumnMap
 */
public class EventAttrColumnService implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EventAttrColumnService.class);
    
    // Key 前缀
    private static final String KEY_PREFIX = "ea:";
    
    // 配置
    private final String host;
    private final int port;
    private final boolean clusterMode;
    private final int localCacheSize;
    private final int localCacheExpireMinutes;
    
    // L1 本地缓存 (transient，每个 TaskManager 独立)
    private transient Cache<String, Integer> localCache;
    
    // KVRocks 客户端
    private transient KvrocksClient kvrocksClient;
    
    // 统计
    private final AtomicLong l1Hits = new AtomicLong(0);
    private final AtomicLong l2Hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);
    
    public EventAttrColumnService(String host, int port, boolean clusterMode,
                                   int localCacheSize, int localCacheExpireMinutes) {
        this.host = host;
        this.port = port;
        this.clusterMode = clusterMode;
        this.localCacheSize = localCacheSize;
        this.localCacheExpireMinutes = localCacheExpireMinutes;
    }
    
    /**
     * 初始化 (在 Flink open() 中调用)
     */
    public void init() {
        // 初始化本地缓存
        localCache = Caffeine.newBuilder()
                .maximumSize(localCacheSize)
                .expireAfterWrite(localCacheExpireMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build();
        
        // 初始化 KVRocks 客户端
        kvrocksClient = new KvrocksClient(host, port, clusterMode);
        kvrocksClient.init();
        
        LOG.info("EventAttrColumnService initialized: host={}, port={}, cluster={}", host, port, clusterMode);
    }
    
    /**
     * 构建 KVRocks Key
     * 
     * @param eventId 事件 ID (对应 Scala 的 eid)
     * @param attrId 属性 ID (对应 Scala 的 pid)
     * @return Key: ea:{eventId}_{attrId}
     */
    private String buildKey(String eventId, String attrId) {
        return KEY_PREFIX + eventId + "_" + attrId;
    }
    
    /**
     * 同步获取列索引
     * 
     * @param eventId 事件 ID
     * @param attrId 属性 ID
     * @return 列索引 (1-100)，未找到返回 -1
     */
    public int getColumnIndex(String eventId, String attrId) {
        if (eventId == null || attrId == null) {
            return -1;
        }
        
        String key = buildKey(eventId, attrId);
        
        // L1 本地缓存查询
        Integer cached = localCache.getIfPresent(key);
        if (cached != null) {
            l1Hits.incrementAndGet();
            return cached;
        }
        
        // L2 KVRocks 同步查询
        try {
            String value = kvrocksClient.asyncGet(key).get(5, TimeUnit.SECONDS);
            if (value != null && !value.isEmpty()) {
                int index = parseColumnIndex(value);
                if (index > 0) {
                    localCache.put(key, index);
                    l2Hits.incrementAndGet();
                    return index;
                }
            }
        } catch (Exception e) {
            LOG.warn("KVRocks GET failed for key: {}", key, e);
        }
        
        misses.incrementAndGet();
        return -1;
    }
    
    /**
     * 异步获取列索引
     */
    public CompletableFuture<Integer> getColumnIndexAsync(String eventId, String attrId) {
        if (eventId == null || attrId == null) {
            return CompletableFuture.completedFuture(-1);
        }
        
        String key = buildKey(eventId, attrId);
        
        // L1 本地缓存查询
        Integer cached = localCache.getIfPresent(key);
        if (cached != null) {
            l1Hits.incrementAndGet();
            return CompletableFuture.completedFuture(cached);
        }
        
        // L2 KVRocks 异步查询
        return kvrocksClient.asyncGet(key)
                .thenApply(value -> {
                    if (value != null && !value.isEmpty()) {
                        int index = parseColumnIndex(value);
                        if (index > 0) {
                            localCache.put(key, index);
                            l2Hits.incrementAndGet();
                            return index;
                        }
                    }
                    misses.incrementAndGet();
                    return -1;
                })
                .exceptionally(ex -> {
                    LOG.warn("KVRocks async GET failed for key: {}", key, ex);
                    misses.incrementAndGet();
                    return -1;
                });
    }
    
    /**
     * 批量异步预加载
     * 
     * @param pairs eventId-attrId 对列表，格式: [[eventId, attrId], ...]
     */
    public CompletableFuture<Map<String, Integer>> preloadAsync(List<String[]> pairs) {
        if (pairs == null || pairs.isEmpty()) {
            return CompletableFuture.completedFuture(new HashMap<String, Integer>());
        }
        
        Map<String, Integer> result = new HashMap<String, Integer>();
        List<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();
        
        for (String[] pair : pairs) {
            if (pair == null || pair.length != 2) {
                continue;
            }
            
            final String eventId = pair[0];
            final String attrId = pair[1];
            final String key = buildKey(eventId, attrId);
            
            // 先检查本地缓存
            Integer cached = localCache.getIfPresent(key);
            if (cached != null) {
                result.put(key, cached);
                continue;
            }
            
            // 异步查询
            futures.add(kvrocksClient.asyncGet(key)
                    .thenAccept(value -> {
                        if (value != null && !value.isEmpty()) {
                            int index = parseColumnIndex(value);
                            if (index > 0) {
                                localCache.put(key, index);
                                synchronized (result) {
                                    result.put(key, index);
                                }
                            }
                        }
                    }));
        }
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> result);
    }
    
    /**
     * 解析列索引
     * 
     * @param value KVRocks 值，格式: cus1 / cus2 / ... / cus100
     * @return 列索引 (1-100)，解析失败返回 -1
     */
    private int parseColumnIndex(String value) {
        if (value == null || !value.startsWith("cus")) {
            return -1;
        }
        
        try {
            return Integer.parseInt(value.substring(3));
        } catch (NumberFormatException e) {
            LOG.warn("Invalid column value: {}", value);
            return -1;
        }
    }
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("EventAttrColumnService Stats: L1 hits=%d, L2 hits=%d, misses=%d",
                l1Hits.get(), l2Hits.get(), misses.get());
    }
    
    /**
     * 关闭资源
     */
    public void close() {
        LOG.info("Closing EventAttrColumnService - {}", getStats());
        if (kvrocksClient != null) {
            kvrocksClient.shutdown();
        }
    }
    
    // Getters for stats
    public long getL1Hits() { return l1Hits.get(); }
    public long getL2Hits() { return l2Hits.get(); }
    public long getMisses() { return misses.get(); }
}
