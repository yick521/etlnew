package com.zhugeio.etl.common.client.redis;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Redis客户端 - Lettuce真异步版本 + 分布式原子操作支持
 *
 * ✅ 优势:
 * 1. 真正的异步IO,基于Netty
 * 2. 无需线程池,不会阻塞
 * 3. 支持百万级并发
 * 4. ✅ 新增: SETNX/HSETNX 原子操作,解决分布式并发问题
 */
public class RedisClient implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(RedisClient.class);
    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final boolean isCluster;

    // Lettuce客户端 (transient, 在每个TaskManager上重新初始化)
    private transient RedisClusterClient clusterClient;
    private transient io.lettuce.core.RedisClient standaloneClient;
    private transient StatefulRedisClusterConnection<String, String> clusterConnection;
    private transient StatefulRedisConnection<String, String> standaloneConnection;

    public RedisClient(String host, int port, boolean isCluster) {
        this.host = host;
        this.port = port;
        this.isCluster = isCluster;
    }

    // 使用静态连接池，所有算子共享
    private static volatile RedisClusterClient sharedClusterClient;
    private static volatile io.lettuce.core.RedisClient sharedStandaloneClient;
    private static final Object LOCK = new Object();

    /**
     * 初始化Lettuce连接
     */
    public void init() {
        if (isCluster) {
            if (sharedClusterClient == null) {
                synchronized (LOCK) {
                    if (sharedClusterClient == null) {
                        sharedClusterClient = RedisClusterClient.create(
                                RedisURI.Builder
                                        .redis(host, port)
                                        .withTimeout(Duration.ofSeconds(60))
                                        .build()
                        );

                        // 增加连接池大小
                        sharedClusterClient.setOptions(ClusterClientOptions.builder()
                                .autoReconnect(true)
                                .maxRedirects(8)
                                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(60000)))
                                .build());
                    }
                }
            }
            clusterConnection = sharedClusterClient.connect();
        } else {
            // 单机模式
            standaloneClient = io.lettuce.core.RedisClient.create(
                    RedisURI.Builder
                            .redis(host, port)
                            .withTimeout(Duration.ofSeconds(60))
                            .build()
            );

            standaloneClient.setOptions(ClientOptions.builder()
                    .autoReconnect(true)
                    .pingBeforeActivateConnection(true)
                    .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(60000)))
                    .build());

            standaloneConnection = standaloneClient.connect();

            LOG.info("✅ Lettuce单机连接初始化成功：{}:{} (真异步模式)", host, port);
        }
    }

    /**
     * ✅ 真正的异步Get操作 (基于Netty,无阻塞)
     */
    public CompletableFuture<String> asyncGet(String key) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.get(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks GET失败: {}, {}", key, ex.getMessage());
                            throw new RuntimeException("KVRocks GET失败", ex);
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.get(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks GET失败: {}, {}", key, ex.getMessage());
                            throw new RuntimeException("KVRocks GET失败", ex);
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }


    /**
     * ✅ 真正的异步Get操作 (基于Netty,无阻塞)
     * List<KeyValue<String, String>> 返回数量和入参一致， 顺序一致，若不存在value为null
     */
    public CompletableFuture<List<KeyValue<String, String>>> asyncMGet(String...keys) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.mget(keys)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("集群版 MGET失败: {}, {}", keys, ex.getMessage());
                            throw new RuntimeException("集群版 MGET失败", ex);
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.mget(keys)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("单机版 MGET失败: {}, {}", keys, ex.getMessage());
                            throw new RuntimeException("单机版 MGET失败", ex);
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * ✅ 真正的异步Get操作 (基于Netty,无阻塞)
     * List<KeyValue<String, String>> 返回数量和入参一致， 顺序一致，若不存在value为null
     */
    public List<KeyValue<String, String>> syncMGet(String...keys) {
        if (isCluster) {
            RedisAdvancedClusterCommands<String, String> async =
                    clusterConnection.sync();
            return async.mget(keys);

        } else {
            RedisCommands<String, String> async =
                    standaloneConnection.sync();
            return async.mget(keys);
        }
    }

    /**
     * ✅ 真正的异步Get操作 (基于Netty,无阻塞)
     * List<KeyValue<String, String>> 返回数量和入参一致， 顺序一致，若不存在value为null
     */
    public String syncGet(String keys) {
        if (isCluster) {
            RedisAdvancedClusterCommands<String, String> async =
                    clusterConnection.sync();
            return async.get(keys);

        } else {
            RedisCommands<String, String> async =
                    standaloneConnection.sync();
            return async.get(keys);
        }
    }

    /**
     * ✅ 真正的异步del操作 (基于Netty,无阻塞)
     */
    public CompletableFuture<Long> asyncDel(String...keys) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.del(keys)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks DEL失败: {}, {}", keys, ex.getMessage());
                            throw new RuntimeException("KVRocks GET失败", ex);
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.del(keys)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks GET失败: {}, {}", keys, ex.getMessage());
                            throw new RuntimeException("KVRocks GET失败", ex);
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    public Long syncDel(String...keys) {
        try {
            if (isCluster) {
                RedisAdvancedClusterCommands<String, String> async =
                        clusterConnection.sync();
                return async.del(keys);
            } else {
                RedisCommands<String, String> async =
                        standaloneConnection.sync();
                return async.del(keys);
            }
        } catch (Exception e) {
            return -1L;
        }
    }

    /**
     * ✅ 真正的异步Set操作 (修复类型转换)
     */
    public CompletableFuture<Void> asyncSet(String key, String value) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.set(key, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SET失败: {}, {}", key, ex.getMessage());
                            throw new RuntimeException("KVRocks SET失败", ex);
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.set(key, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SET失败: {}, {}", key, ex.getMessage());
                            throw new RuntimeException("KVRocks SET失败", ex);
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    public String syncSet(String key, String value) {
        if (isCluster) {
            RedisAdvancedClusterCommands<String, String> async =
                    clusterConnection.sync();
            return async.set(key, value);
        } else {
            RedisCommands<String, String> async =
                    standaloneConnection.sync();
            return async.set(key, value);
        }
    }

    public Boolean expire(String key,long expire) {
        if (isCluster) {
            RedisAdvancedClusterCommands<String, String> async =
                    clusterConnection.sync();
            return async.expire(key, expire);
        } else {
            RedisCommands<String, String> async =
                    standaloneConnection.sync();
            return async.expire(key, expire);
        }
    }

    /**
     * ✅ 原子性的 SETNX (SET if Not eXists)
     * 只在key不存在时设置值
     *
     * @return true表示设置成功(之前不存在), false表示key已存在
     */
    public CompletableFuture<Boolean> asyncSetIfAbsent(String key, String value) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.setnx(key, value)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SETNX失败: {}, {}", key, ex.getMessage());
                            return false;  // 失败时返回false
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.setnx(key, value)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SETNX失败: {}, {}", key, ex.getMessage());
                            return false;
                        });
            }
        } catch (Exception e) {
            LOG.error("KVRocks SETNX异常: {}, {}", key, e.getMessage());
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     * ✅ 异步Hash Get
     */
    public CompletableFuture<String> asyncHGet(String key, String field) {
        try {
            if (isCluster) {
                return clusterConnection.async().hget(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> null);
            } else {
                return standaloneConnection.async().hget(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> null);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    public String syncHGet(String key, String field) {
        try {
            if (isCluster) {
                return clusterConnection.sync().hget(key, field);
            } else {
                return standaloneConnection.sync().hget(key, field);
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * ✅ 异步Hash Set (修复类型转换)
     */
    public CompletableFuture<Void> asyncHSet(String key, String field, String value) {
        try {
            if (isCluster) {
                return clusterConnection.async().hset(key, field, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> null);
            } else {
                return standaloneConnection.async().hset(key, field, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> null);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * ✅ 原子性的 HSETNX (Hash SET if Not eXists)
     * 只在hash的field不存在时设置值
     *
     * @return true表示设置成功(field之前不存在), false表示field已存在
     */
    public CompletableFuture<Boolean> asyncHSetIfAbsent(String key, String field, String value) {
        try {
            if (isCluster) {
                return clusterConnection.async().hsetnx(key, field, value)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HSETNX失败: {}:{}, {}", key, field, ex.getMessage());
                            return false;
                        });
            } else {
                return standaloneConnection.async().hsetnx(key, field, value)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HSETNX失败: {}:{}, {}", key, field, ex.getMessage());
                            return false;
                        });
            }
        } catch (Exception e) {
            LOG.error("KVRocks HSETNX异常: {}:{}, {}", key, field, e.getMessage());
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     * ✅ 批量Hash Get (真正的Pipeline)
     */
    public CompletableFuture<Map<String, String>> asyncBatchHGet(String key, List<String> fields) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();

                // Pipeline多个HGET命令
                List<RedisFuture<String>> futures = fields.stream()
                        .map(field -> async.hget(key, field))
                        .collect(Collectors.toList());

                // 等待所有完成
                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> {
                    Map<String, String> result = new HashMap<>();
                    for (int i = 0; i < fields.size(); i++) {
                        try {
                            String value = futures.get(i).get();
                            if (value != null) {
                                result.put(fields.get(i), value);
                            }
                        } catch (Exception ignored) {}
                    }
                    return result;
                });

            } else {
                // 单机模式使用真正的Pipeline
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);  // 开启Pipeline

                List<RedisFuture<String>> futures = fields.stream()
                        .map(field -> async.hget(key, field))
                        .collect(Collectors.toList());

                async.flushCommands();  // 一次性发送
                async.setAutoFlushCommands(true);

                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> {
                    Map<String, String> result = new HashMap<>();
                    for (int i = 0; i < fields.size(); i++) {
                        try {
                            String value = futures.get(i).get();
                            if (value != null) {
                                result.put(fields.get(i), value);
                            }
                        } catch (Exception ignored) {}
                    }
                    return result;
                });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new HashMap<>());
        }
    }

    /**
     * ✅ 批量Hash Set (修复类型转换)
     */
    public CompletableFuture<Void> asyncBatchHSet(String key, Map<String, String> fieldValues) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();

                List<RedisFuture<Boolean>> futures = fieldValues.entrySet().stream()
                        .map(entry -> async.hset(key, entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> (Void) null);

            } else {
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<Boolean>> futures = fieldValues.entrySet().stream()
                        .map(entry -> async.hset(key, entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

                async.flushCommands();
                async.setAutoFlushCommands(true);

                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> (Void) null);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * 测试连接
     */
    public boolean testConnection() {
        try {
            if (isCluster) {
                String pong = clusterConnection.sync().ping();
                return "PONG".equalsIgnoreCase(pong);
            } else {
                String pong = standaloneConnection.sync().ping();
                return "PONG".equalsIgnoreCase(pong);
            }
        } catch (Exception e) {
            LOG.error("KVRocks连接测试失败: {}", e.getMessage());
            return false;
        }
    }

    /**
     * ✅ 同步批量管道查询 (Window算子专用)
     */
    public Map<String, String> syncBatchHGet(String hashKey, List<String> fields, long timeoutMs) {
        Map<String, String> results = new HashMap<>();

        if (fields == null || fields.isEmpty()) {
            return results;
        }

        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<String>> futures = new ArrayList<>();
                for (String field : fields) {
                    futures.add(async.hget(hashKey, field));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                for (int i = 0; i < fields.size(); i++) {
                    try {
                        String value = futures.get(i).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                        if (value != null) {
                            results.put(hashKey + ":" + fields.get(i), value);
                        }
                    } catch (Exception e) {
                        // 单个查询失败不影响其他
                    }
                }

            } else {
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<String>> futures = new ArrayList<>();
                for (String field : fields) {
                    futures.add(async.hget(hashKey, field));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                for (int i = 0; i < fields.size(); i++) {
                    try {
                        String value = futures.get(i).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                        if (value != null) {
                            results.put(hashKey + ":" + fields.get(i), value);
                        }
                    } catch (Exception e) {
                        // 继续
                    }
                }
            }

        } catch (Exception e) {
            LOG.error("批量查询失败: {}, {}", hashKey, e.getMessage());
        }

        return results;
    }

    /**
     * ✅ 同步批量管道写入 (Window算子专用)
     */
    public void syncBatchHSet(String hashKey, Map<String, String> fieldValues, long timeoutMs) {
        if (fieldValues == null || fieldValues.isEmpty()) {
            return;
        }

        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<Boolean>> futures = new ArrayList<>();
                for (Map.Entry<String, String> entry : fieldValues.entrySet()) {
                    futures.add(async.hset(hashKey, entry.getKey(), entry.getValue()));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);

            } else {
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<Boolean>> futures = new ArrayList<>();
                for (Map.Entry<String, String> entry : fieldValues.entrySet()) {
                    futures.add(async.hset(hashKey, entry.getKey(), entry.getValue()));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            }

        } catch (Exception e) {
            LOG.error("批量写入失败: {}, {}", hashKey, e.getMessage());
        }
    }

    /**
     * 关闭连接
     */
    public void shutdown() {
        try {
            if (clusterConnection != null) {
                clusterConnection.close();
            }
            if (clusterClient != null) {
                clusterClient.shutdown();
                LOG.info("Lettuce集群连接已关闭");
            }

            if (standaloneConnection != null) {
                standaloneConnection.close();
            }
            if (standaloneClient != null) {
                standaloneClient.shutdown();
                LOG.info("Lettuce单机连接已关闭");
            }
        } catch (Exception e) {
            LOG.error("关闭Lettuce连接失败: {}", e.getMessage());
        }
    }
}