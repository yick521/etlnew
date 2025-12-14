package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.util.ip.IpDatabaseLoader;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * IP 地理位置富化算子
 * 
 * 将 ZGMessage 中的 IP 地址解析为地理位置信息 (支持 IPv4 + IPv6)
 * 结果写入 message.data 中的 country/province/city 字段
 * 
 * IPv4: 使用简单二进制格式数据库
 * IPv6: 使用 AWDB 格式数据库 (ipplus360.com)
 */
public class IpEnrichOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IpEnrichOperator.class);

    private transient IpDatabaseLoader ipLoader;

    // IPv4 配置
    private final String ipv4HdfsPath;
    
    // IPv6 配置
    private final String ipv6HdfsPath;
    private final boolean enableIpv6;
    
    // 通用配置
    private final boolean enableAutoReload;
    private final long reloadIntervalSeconds;
    private final boolean isHdfsHA;
    private final String hdfsNameservice;
    private final String hdfsNamenodes;

    /**
     * 构造函数 (仅 IPv4，向后兼容)
     */
    public IpEnrichOperator(String hdfsPath, boolean enableAutoReload, 
                            long reloadIntervalSeconds, boolean isHdfsHA) {
        this(hdfsPath, null, false, enableAutoReload, reloadIntervalSeconds, isHdfsHA, null, null);
    }

    /**
     * 构造函数 (IPv4 + IPv6)
     */
    public IpEnrichOperator(String ipv4HdfsPath, String ipv6HdfsPath, boolean enableIpv6,
                            boolean enableAutoReload, long reloadIntervalSeconds, 
                            boolean isHdfsHA, String hdfsNameservice, String hdfsNamenodes) {
        this.ipv4HdfsPath = ipv4HdfsPath;
        this.ipv6HdfsPath = ipv6HdfsPath;
        this.enableIpv6 = enableIpv6;
        this.enableAutoReload = enableAutoReload;
        this.reloadIntervalSeconds = reloadIntervalSeconds;
        this.isHdfsHA = isHdfsHA;
        this.hdfsNameservice = hdfsNameservice;
        this.hdfsNamenodes = hdfsNamenodes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // 使用 Builder 模式初始化 IP 数据库加载器
        IpDatabaseLoader.Builder builder = IpDatabaseLoader.builder()
            .ipv4HdfsPath(ipv4HdfsPath)
            .enableHotReload(enableAutoReload)
            .reloadIntervalSeconds(reloadIntervalSeconds)
            .enableIpv6(enableIpv6);
        
        // IPv6 路径
        if (enableIpv6 && ipv6HdfsPath != null && !ipv6HdfsPath.isEmpty()) {
            builder.ipv6HdfsPath(ipv6HdfsPath);
        }
        
        // HDFS HA 配置
        if (isHdfsHA && hdfsNameservice != null) {
            builder.hdfsHA(true, hdfsNameservice, hdfsNamenodes);
        }

        ipLoader = builder.build();
        ipLoader.init();

        LOG.info("[IP富化算子-{}] 初始化成功, IPv4路径: {}, IPv6路径: {}, IPv6启用: {}", 
                subtaskIndex, ipv4HdfsPath, ipv6HdfsPath, enableIpv6);
    }

    @Override
    public void asyncInvoke(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        
        CompletableFuture.supplyAsync(() -> {
            try {
                // 从 message.data 中获取 IP
                Map<String, Object> data = message.getData();
                if (data == null) {
                    return message;
                }

                Object ipObj = data.get("ip");
                if (ipObj == null) {
                    return message;
                }

                String ip = String.valueOf(ipObj);
                if (ip.isEmpty() || "null".equals(ip) || "0.0.0.0".equals(ip) || "::".equals(ip)) {
                    return message;
                }

                // 查询 IP 数据库 (自动识别 IPv4/IPv6)
                String[] result = ipLoader.find(ip);

                if (result != null && result.length >= 3) {
                    // 将结果写入 data
                    data.put("country", result[0] != null ? result[0] : "");
                    data.put("province", result[1] != null ? result[1] : "");
                    data.put("city", result[2] != null ? result[2] : "");

                    // 如果有第4个元素，可能是 ISP
                    if (result.length >= 4 && result[3] != null) {
                        data.put("isp", result[3]);
                    }
                }

            } catch (Exception e) {
                LOG.error("[IP富化算子] 处理失败: {}", e.getMessage());
            }

            return message;

        }).whenComplete((result, throwable) -> {
            if (throwable != null) {
                LOG.error("[IP富化算子] 异步处理异常", throwable);
                resultFuture.complete(Collections.singleton(message));
            } else {
                resultFuture.complete(Collections.singleton(result));
            }
        });
    }

    @Override
    public void timeout(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[IP富化算子] 处理超时, offset={}", message.getOffset());
        resultFuture.complete(Collections.singleton(message));
    }

    @Override
    public void close() throws Exception {
        if (ipLoader != null) {
            ipLoader.close();
            LOG.info("[IP富化算子] IP 数据库已关闭");
        }
    }
    
    /**
     * 检查 IPv6 是否可用
     */
    public boolean isIpv6Available() {
        return ipLoader != null && ipLoader.isIpv6Enabled();
    }
}
