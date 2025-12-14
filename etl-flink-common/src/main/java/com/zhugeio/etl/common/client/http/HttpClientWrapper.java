package com.zhugeio.etl.common.client.http;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 通用 HTTP 客户端 (JDK 8 兼容版本)
 * 
 * 使用 Apache HttpClient，支持:
 * - 连接池
 * - 同步和异步请求
 * - 自动重试
 * - TaskManager 内静态单例复用
 */
public class HttpClientWrapper {
    
    private static final Logger LOG = LoggerFactory.getLogger(HttpClientWrapper.class);
    
    // 静态单例
    private static volatile HttpClientWrapper INSTANCE;
    private static final Object LOCK = new Object();
    
    // HTTP 客户端
    private final CloseableHttpClient httpClient;
    private final PoolingHttpClientConnectionManager connectionManager;
    
    // 异步执行线程池
    private final ExecutorService asyncExecutor;
    
    // 配置
    private final int connectTimeout;
    private final int socketTimeout;
    private final int maxRetries;
    
    // 统计
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong successRequests = new AtomicLong(0);
    private final AtomicLong failedRequests = new AtomicLong(0);
    private final AtomicLong asyncRequests = new AtomicLong(0);
    
    /**
     * 私有构造函数
     * 
     * @param connectTimeout 连接超时(毫秒)
     * @param socketTimeout 读取超时(毫秒)
     * @param maxRetries 最大重试次数
     * @param maxConnTotal 最大连接数
     * @param maxConnPerRoute 每个路由最大连接数
     * @param asyncThreadPoolSize 异步线程池大小
     */
    private HttpClientWrapper(int connectTimeout, int socketTimeout, int maxRetries,
                               int maxConnTotal, int maxConnPerRoute, int asyncThreadPoolSize) {
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
        this.maxRetries = maxRetries;
        
        // 创建连接池管理器
        connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(maxConnTotal);
        connectionManager.setDefaultMaxPerRoute(maxConnPerRoute);
        
        // 请求配置
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .setConnectionRequestTimeout(connectTimeout)
                .build();
        
        // 创建 HTTP 客户端
        httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(requestConfig)
                .build();
        
        // 创建异步线程池
        asyncExecutor = new ThreadPoolExecutor(
                asyncThreadPoolSize / 2,
                asyncThreadPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "http-async-pool");
                        t.setDaemon(true);
                        return t;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        
        LOG.info("HttpClientWrapper initialized: connectTimeout={}ms, socketTimeout={}ms, " +
                 "maxRetries={}, maxConnTotal={}, maxConnPerRoute={}, asyncThreadPoolSize={}",
                connectTimeout, socketTimeout, maxRetries, maxConnTotal, maxConnPerRoute, asyncThreadPoolSize);
    }
    
    /**
     * 获取单例实例 (使用默认配置)
     */
    public static HttpClientWrapper getInstance() {
        return getInstance(5000, 10000, 3, 200, 50, 10);
    }
    
    /**
     * 获取单例实例 (自定义配置)
     * 
     * @param connectTimeoutMs 连接超时(毫秒)
     * @param socketTimeoutMs 读取超时(毫秒)
     * @param maxRetries 最大重试次数
     * @param maxConnTotal 最大连接数
     * @param maxConnPerRoute 每个路由最大连接数
     * @param asyncThreadPoolSize 异步线程池大小
     */
    public static HttpClientWrapper getInstance(int connectTimeoutMs, int socketTimeoutMs,
                                                 int maxRetries, int maxConnTotal,
                                                 int maxConnPerRoute, int asyncThreadPoolSize) {
        if (INSTANCE == null) {
            synchronized (LOCK) {
                if (INSTANCE == null) {
                    INSTANCE = new HttpClientWrapper(connectTimeoutMs, socketTimeoutMs, maxRetries,
                            maxConnTotal, maxConnPerRoute, asyncThreadPoolSize);
                }
            }
        }
        return INSTANCE;
    }
    
    /**
     * 简化版获取实例 (秒为单位)
     * 
     * @param connectTimeoutSec 连接超时(秒)
     * @param socketTimeoutSec 读取超时(秒)
     * @param maxRetries 最大重试次数
     * @param threadPoolSize 线程池大小
     */
    public static HttpClientWrapper getInstance(int connectTimeoutSec, int socketTimeoutSec,
                                                 int maxRetries, int threadPoolSize) {
        return getInstance(connectTimeoutSec * 1000, socketTimeoutSec * 1000, maxRetries,
                200, 50, threadPoolSize);
    }
    
    // ==================== 同步请求 ====================
    
    /**
     * 同步 GET 请求
     */
    public HttpResult get(String url) {
        return get(url, null);
    }
    
    /**
     * 同步 GET 请求 (带请求头)
     */
    public HttpResult get(String url, Map<String, String> headers) {
        totalRequests.incrementAndGet();
        
        HttpGet httpGet = new HttpGet(url);
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpGet.setHeader(entry.getKey(), entry.getValue());
            }
        }
        
        return executeWithRetry(httpGet);
    }
    
    /**
     * 同步 POST 请求
     */
    public HttpResult post(String url, String body, Map<String, String> headers) {
        totalRequests.incrementAndGet();
        
        HttpPost httpPost = new HttpPost(url);
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpPost.setHeader(entry.getKey(), entry.getValue());
            }
        }
        if (body != null) {
            httpPost.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
        }
        
        return executeWithRetry(httpPost);
    }
    
    /**
     * 带重试的请求执行
     */
    private HttpResult executeWithRetry(HttpRequestBase request) {
        Exception lastException = null;
        
        for (int retry = 0; retry <= maxRetries; retry++) {
            CloseableHttpResponse response = null;
            try {
                response = httpClient.execute(request);
                int statusCode = response.getStatusLine().getStatusCode();
                HttpEntity entity = response.getEntity();
                String body = entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : null;
                
                successRequests.incrementAndGet();
                return new HttpResult(statusCode, body, null);
                
            } catch (IOException e) {
                lastException = e;
                if (retry < maxRetries) {
                    LOG.debug("HTTP request retry {}/{}: {}", retry + 1, maxRetries, request.getURI());
                    try {
                        Thread.sleep(100 * (retry + 1)); // 简单退避
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } finally {
                if (response != null) {
                    try {
                        response.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
        
        failedRequests.incrementAndGet();
        LOG.warn("HTTP request failed after {} retries: {}", maxRetries, request.getURI(), lastException);
        return new HttpResult(-1, null, lastException);
    }
    
    // ==================== 异步请求 ====================
    
    /**
     * 异步 GET 请求
     */
    public CompletableFuture<HttpResult> getAsync(String url) {
        return getAsync(url, null);
    }
    
    /**
     * 异步 GET 请求 (带请求头)
     */
    public CompletableFuture<HttpResult> getAsync(final String url, final Map<String, String> headers) {
        asyncRequests.incrementAndGet();
        
        final CompletableFuture<HttpResult> future = new CompletableFuture<HttpResult>();
        
        asyncExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    HttpResult result = get(url, headers);
                    future.complete(result);
                } catch (Exception e) {
                    future.complete(new HttpResult(-1, null, e));
                }
            }
        });
        
        return future;
    }
    
    /**
     * 异步 POST 请求
     */
    public CompletableFuture<HttpResult> postAsync(final String url, final String body, 
                                                    final Map<String, String> headers) {
        asyncRequests.incrementAndGet();
        
        final CompletableFuture<HttpResult> future = new CompletableFuture<HttpResult>();
        
        asyncExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    HttpResult result = post(url, body, headers);
                    future.complete(result);
                } catch (Exception e) {
                    future.complete(new HttpResult(-1, null, e));
                }
            }
        });
        
        return future;
    }
    
    // ==================== 管理方法 ====================
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("HttpClient Stats: total=%d, success=%d, failed=%d, async=%d, " +
                             "poolAvailable=%d, poolLeased=%d",
                totalRequests.get(), successRequests.get(), failedRequests.get(), asyncRequests.get(),
                connectionManager.getTotalStats().getAvailable(),
                connectionManager.getTotalStats().getLeased());
    }
    
    /**
     * 关闭客户端 (通常不需要手动调用)
     */
    public void close() {
        LOG.info("Closing HttpClientWrapper - {}", getStats());
        
        try {
            asyncExecutor.shutdown();
            if (!asyncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                asyncExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            asyncExecutor.shutdownNow();
        }
        
        try {
            httpClient.close();
        } catch (IOException e) {
            LOG.error("Error closing HttpClient", e);
        }
        
        connectionManager.close();
    }
    
    // Getters for stats
    public long getTotalRequests() { return totalRequests.get(); }
    public long getSuccessRequests() { return successRequests.get(); }
    public long getFailedRequests() { return failedRequests.get(); }
    public long getAsyncRequests() { return asyncRequests.get(); }
    
    /**
     * HTTP 请求结果
     */
    public static class HttpResult {
        private final int statusCode;
        private final String body;
        private final Throwable error;
        
        public HttpResult(int statusCode, String body, Throwable error) {
            this.statusCode = statusCode;
            this.body = body;
            this.error = error;
        }
        
        public boolean isSuccess() {
            return statusCode >= 200 && statusCode < 300;
        }
        
        public int getStatusCode() { return statusCode; }
        public String getBody() { return body; }
        public Throwable getError() { return error; }
        
        @Override
        public String toString() {
            return "HttpResult{statusCode=" + statusCode + ", bodyLength=" + 
                   (body != null ? body.length() : 0) + ", error=" + error + "}";
        }
    }
}
