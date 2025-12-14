package com.zhugeio.etl.pipeline.operator.dw;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.zhugeio.etl.common.model.ua.UserAgentInfo;
import com.zhugeio.etl.common.util.ua.UserAgentParser;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * User-Agent 解析富化算子
 * 
 * 将 ZGMessage 中的 User-Agent 字符串解析为结构化信息
 * 结果写入 message.data 中的 os/browser/brand/model 等字段
 * 
 * 使用 Caffeine 本地缓存提高性能（UA 重复率高）
 */
public class UserAgentEnrichOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(UserAgentEnrichOperator.class);

    private transient UserAgentParser parser;
    private transient Cache<String, UserAgentInfo> uaCache;

    private final int cacheSize;
    private final long expireMinutes;

    public UserAgentEnrichOperator() {
        this(10000, 60);  // 默认缓存1万条，1小时过期
    }

    public UserAgentEnrichOperator(int cacheSize, long expireMinutes) {
        this.cacheSize = cacheSize;
        this.expireMinutes = expireMinutes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // 初始化 UA 解析器
        parser = new UserAgentParser();

        // 初始化 Caffeine 缓存
        uaCache = Caffeine.newBuilder()
            .maximumSize(cacheSize)
            .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
            .recordStats()
            .build();

        LOG.info("[UA富化算子-{}] 初始化成功, 缓存大小={}, 过期时间={}分钟",
            subtaskIndex, cacheSize, expireMinutes);
    }

    @Override
    public void asyncInvoke(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        
        CompletableFuture.supplyAsync(() -> {
            try {
                // 从 message.data 中获取 UA
                Map<String, Object> data = message.getData();
                if (data == null) {
                    return message;
                }

                Object uaObj = data.get("ua");
                if (uaObj == null) {
                    return message;
                }

                String ua = String.valueOf(uaObj);
                if (ua.isEmpty() || "null".equals(ua)) {
                    return message;
                }

                // 先查缓存
                UserAgentInfo uaInfo = uaCache.getIfPresent(ua);

                if (uaInfo == null) {
                    // 缓存未命中，解析 UA
                    uaInfo = parser.parse(ua);
                    uaCache.put(ua, uaInfo);
                }

                // 将结果写入 data
                if (uaInfo != null) {
                    data.put("os", uaInfo.getOsName());
                    data.put("os_version", uaInfo.getOsVersion());
                    data.put("browser", uaInfo.getBrowserName());
                    data.put("browser_version", uaInfo.getBrowserVersion());
                    data.put("device_type", uaInfo.getDeviceType());

                    // 设备品牌和型号（如果 UA 能解析出来）
                    if (uaInfo.getDeviceBrand() != null) {
                        data.put("brand", uaInfo.getDeviceBrand());
                    }
                    if (uaInfo.getDeviceModel() != null) {
                        data.put("model", uaInfo.getDeviceModel());
                    }
                }

            } catch (Exception e) {
                LOG.error("[UA富化算子] 处理失败: {}", e.getMessage());
            }

            return message;

        }).whenComplete((result, throwable) -> {
            if (throwable != null) {
                LOG.error("[UA富化算子] 异步处理异常", throwable);
                resultFuture.complete(Collections.singleton(message));
            } else {
                resultFuture.complete(Collections.singleton(result));
            }
        });
    }

    @Override
    public void timeout(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[UA富化算子] 处理超时, offset={}", message.getOffset());
        resultFuture.complete(Collections.singleton(message));
    }

    @Override
    public void close() throws Exception {
        if (uaCache != null) {
            // 打印缓存统计
            CacheStats stats = uaCache.stats();
            LOG.info("[UA富化算子] 缓存统计: 命中率={}, 请求数={}, 命中数={}, 未命中数={}",
                String.format("%.2f%%", stats.hitRate() * 100),
                stats.requestCount(),
                stats.hitCount(),
                stats.missCount());

            uaCache.invalidateAll();
        }
    }
}
