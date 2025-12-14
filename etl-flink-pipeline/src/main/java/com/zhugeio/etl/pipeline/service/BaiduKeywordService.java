package com.zhugeio.etl.pipeline.service;

import com.zhugeio.etl.common.client.http.HttpClientWrapper;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.Serializable;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 百度关键词查询服务
 * 
 * 业务逻辑:
 * - 从百度 URL 中提取 eqid
 * - 通过百度 API 查询对应的关键词
 * - 使用三级缓存: L1 本地 → L2 Redis → L3 百度 API
 * 
 * 支持的事件类型: evt, ss, se, abp (不包括 vtl, mkt)
 * 
 * 对应 Scala: MsgParseService + BaiBuKeyWordService
 */
public class BaiduKeywordService implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BaiduKeywordService.class);
    
    // 百度域名
    private static final String WWW_BAIDU_COM = ".baidu.com";
    
    // 需要提取关键词的事件类型
    private static final Set<String> KEYWORD_DT_TYPES = new HashSet<String>(Arrays.asList("evt", "ss", "se", "abp"));
    
    // 配置
    private final String baiduUrl;
    private final String baiduId;
    private final String baiduKey;
    private final String redisHost;
    private final int redisPort;
    private final boolean redisCluster;
    private final int requestSocketTimeout;
    private final int requestConnectTimeout;
    
    // L1 本地缓存 (transient)
    private transient Map<String, String> localCache;
    
    // 客户端
    private transient HttpClientWrapper httpClient;
    private transient KvrocksClient redisClient;
    
    // 统计
    private final AtomicLong l1Hits = new AtomicLong(0);
    private final AtomicLong l2Hits = new AtomicLong(0);
    private final AtomicLong l3Hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);
    
    public BaiduKeywordService(String baiduUrl, String baiduId, String baiduKey,
                                String redisHost, int redisPort, boolean redisCluster,
                                int requestSocketTimeout, int requestConnectTimeout) {
        this.baiduUrl = baiduUrl;
        this.baiduId = baiduId;
        this.baiduKey = baiduKey;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisCluster = redisCluster;
        this.requestSocketTimeout = requestSocketTimeout;
        this.requestConnectTimeout = requestConnectTimeout;
    }
    
    /**
     * 初始化
     */
    public void init() {
        // 初始化本地缓存
        localCache = new ConcurrentHashMap<String, String>(5000);
        
        // 获取 HTTP 客户端单例
        httpClient = HttpClientWrapper.getInstance(requestSocketTimeout, requestConnectTimeout, 3, 10);
        
        // 初始化 Redis 客户端 (用于关键词缓存)
        redisClient = new KvrocksClient(redisHost, redisPort, redisCluster);
        redisClient.init();
        
        LOG.info("BaiduKeywordService initialized: baiduUrl={}", baiduUrl);
    }
    
    /**
     * 检查事件类型是否需要提取关键词
     */
    public boolean shouldExtractKeyword(String dt) {
        return KEYWORD_DT_TYPES.contains(dt);
    }
    
    /**
     * 从 referrer URL 提取 eqid
     */
    public String extractEqid(String referrerUrl) {
        if (referrerUrl == null || referrerUrl.isEmpty() || "\\N".equals(referrerUrl)) {
            return null;
        }
        
        try {
            URI uri = new URI(referrerUrl);
            String host = uri.getHost();
            
            // 只处理百度域名
            if (host == null || !host.endsWith(WWW_BAIDU_COM)) {
                return null;
            }
            
            // 解析查询参数
            String query = uri.getQuery();
            if (query == null) {
                return null;
            }
            
            Map<String, String> params = parseQueryParams(query);
            String eqid = params.get("eqid");
            
            // 验证 eqid 长度
            if (eqid != null && eqid.trim().length() >= 32) {
                return eqid.trim();
            }
            
        } catch (Exception e) {
            LOG.debug("Failed to extract eqid from: {}", referrerUrl);
        }
        
        return null;
    }
    
    /**
     * 异步获取关键词
     */
    public CompletableFuture<String> getKeywordAsync(final String eqid) {
        if (eqid == null || eqid.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        
        // L1 本地缓存查询
        String cached = localCache.get(eqid);
        if (cached != null) {
            l1Hits.incrementAndGet();
            return CompletableFuture.completedFuture(cached);
        }
        
        // L2 Redis 异步查询
        return redisClient.asyncGet(eqid)
                .thenCompose(keyword -> {
                    if (keyword != null && !keyword.isEmpty()) {
                        l2Hits.incrementAndGet();
                        localCache.put(eqid, keyword);
                        return CompletableFuture.completedFuture(keyword);
                    }
                    
                    // L3 百度 API 异步查询
                    return fetchFromBaiduAsync(eqid);
                })
                .exceptionally(ex -> {
                    LOG.warn("Failed to get keyword for eqid: {}", eqid, ex);
                    misses.incrementAndGet();
                    return null;
                });
    }
    
    /**
     * 同步获取关键词
     */
    public String getKeyword(String eqid) {
        if (eqid == null || eqid.isEmpty()) {
            return null;
        }
        
        // L1 本地缓存查询
        String cached = localCache.get(eqid);
        if (cached != null) {
            l1Hits.incrementAndGet();
            return cached;
        }
        
        // L2 Redis 同步查询
        try {
            String keyword = redisClient.asyncGet(eqid).get(5, java.util.concurrent.TimeUnit.SECONDS);
            if (keyword != null && !keyword.isEmpty()) {
                l2Hits.incrementAndGet();
                localCache.put(eqid, keyword);
                return keyword;
            }
        } catch (Exception e) {
            LOG.warn("Redis GET failed for eqid: {}", eqid, e);
        }
        
        // L3 百度 API 查询
        String keyword = fetchFromBaidu(eqid);
        if (keyword != null && !keyword.isEmpty()) {
            l3Hits.incrementAndGet();
            localCache.put(eqid, keyword);
            redisClient.asyncSet(eqid, keyword); // 异步写回 Redis
            return keyword;
        }
        
        misses.incrementAndGet();
        return null;
    }
    
    /**
     * 批量预加载关键词
     */
    public CompletableFuture<Map<String, String>> preloadKeywordsAsync(Set<String> eqids) {
        if (eqids == null || eqids.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.<String, String>emptyMap());
        }
        
        final Map<String, String> result = new ConcurrentHashMap<String, String>();
        
        List<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();
        for (final String eqid : eqids) {
            futures.add(getKeywordAsync(eqid)
                    .thenAccept(keyword -> {
                        if (keyword != null) {
                            result.put(eqid, keyword);
                        }
                    }));
        }
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> result);
    }
    
    /**
     * 同步调用百度 API
     */
    private String fetchFromBaidu(String eqid) {
        try {
            String url = baiduUrl + "/" + eqid;
            Map<String, String> headers = generateBaiduAuthHeaders(eqid);
            
            HttpClientWrapper.HttpResult result = httpClient.get(url, headers);
            if (result.isSuccess()) {
                return parseKeywordFromResponse(result.getBody());
            }
        } catch (Exception e) {
            LOG.warn("Failed to fetch from Baidu API: {}", eqid, e);
        }
        return null;
    }
    
    /**
     * 异步调用百度 API
     */
    private CompletableFuture<String> fetchFromBaiduAsync(final String eqid) {
        try {
            String url = baiduUrl + "/" + eqid;
            Map<String, String> headers = generateBaiduAuthHeaders(eqid);
            
            return httpClient.getAsync(url, headers)
                    .thenApply(result -> {
                        if (result.isSuccess()) {
                            String keyword = parseKeywordFromResponse(result.getBody());
                            if (keyword != null && !keyword.isEmpty()) {
                                l3Hits.incrementAndGet();
                                localCache.put(eqid, keyword);
                                redisClient.asyncSet(eqid, keyword);
                                return keyword;
                            }
                        }
                        misses.incrementAndGet();
                        return null;
                    });
        } catch (Exception e) {
            LOG.warn("Failed to build Baidu request: {}", eqid, e);
            return CompletableFuture.completedFuture(null);
        }
    }
    
    /**
     * 生成百度 API 认证头
     * 对应 Scala: BceV1Signer.getAuth
     */
    private Map<String, String> generateBaiduAuthHeaders(String eqid) {
        Map<String, String> headers = new LinkedHashMap<String, String>();
        
        String host = "referer.bj.baidubce.com";
        headers.put("accept-encoding", "gzip, deflate");
        headers.put("host", host);
        headers.put("content-type", "application/json");
        headers.put("accept", "*/*");
        
        // UTC 时间
        String utcTimeStr = DateTimeFormatter.ISO_INSTANT
                .format(Instant.now().atOffset(ZoneOffset.UTC));
        headers.put("x-bce-date", utcTimeStr);
        
        // 生成签名
        String authString = "bce-auth-v1/" + baiduId + "/" + utcTimeStr + "/1800";
        String signingKey = sha256Hex(baiduKey, authString);
        String canonicalUri = "/v1/eqid/" + eqid;
        String canonicalHeader = "host:" + host;
        String canonicalRequest = "GET\n" + canonicalUri + "\n\n" + canonicalHeader;
        String signature = sha256Hex(signingKey, canonicalRequest);
        String authorization = authString + "/host/" + signature;
        
        headers.put("authorization", authorization);
        
        return headers;
    }
    
    private String sha256Hex(String key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] result = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(result);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate signature", e);
        }
    }
    
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    private String parseKeywordFromResponse(String responseBody) {
        try {
            // 简单 JSON 解析
            // 响应格式: {"eqid":"xxx","wd":"关键词"}
            if (responseBody != null && responseBody.contains("\"wd\"")) {
                int start = responseBody.indexOf("\"wd\"") + 6;
                int end = responseBody.indexOf("\"", start);
                if (start > 6 && end > start) {
                    String keyword = responseBody.substring(start, end);
                    return URLDecoder.decode(keyword, "UTF-8");
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse Baidu response: {}", responseBody, e);
        }
        return null;
    }
    
    private Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new HashMap<String, String>();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            if (idx > 0) {
                try {
                    String key = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
                    String value = idx < pair.length() - 1 
                            ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") 
                            : "";
                    params.put(key, value);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        return params;
    }
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("BaiduKeywordService Stats: L1=%d, L2=%d, L3=%d, miss=%d",
                l1Hits.get(), l2Hits.get(), l3Hits.get(), misses.get());
    }
    
    /**
     * 关闭资源
     */
    public void close() {
        LOG.info("Closing BaiduKeywordService - {}", getStats());
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
    
    // Getters for stats
    public long getL1Hits() { return l1Hits.get(); }
    public long getL2Hits() { return l2Hits.get(); }
    public long getL3Hits() { return l3Hits.get(); }
    public long getMisses() { return misses.get(); }
}
