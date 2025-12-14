package com.zhugeio.etl.common.util.ip;

import com.zhugeio.etl.common.model.ip.IpDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IP数据库加载器
 * 支持从HDFS加载IPv4和IPv6数据库，支持热更新
 * 
 * 对应 Scala: IP.java + AWReader.java
 */
public class IpDatabaseLoader implements Closeable, Serializable {
    
    private static final Logger LOG = LoggerFactory.getLogger(IpDatabaseLoader.class);
    private static final long serialVersionUID = 1L;

    // IPv4 数据库
    private final AtomicReference<IpDatabase> ipv4DatabaseRef = new AtomicReference<>();
    
    // IPv6 数据库 (AWDB格式)
    private final AtomicReference<AwdbReader> ipv6DatabaseRef = new AtomicReference<>();

    // HDFS 配置
    private final String ipv4HdfsPath;
    private final String ipv6HdfsPath;
    private final boolean isHdfsHA;
    private final String hdfsNameservice;
    private final String hdfsNamenodes;

    // 热更新配置
    private final boolean enableHotReload;
    private final long reloadIntervalSeconds;
    private volatile long ipv4LastModifyTime = 0L;
    private volatile long ipv6LastModifyTime = 0L;
    private transient ScheduledExecutorService scheduler;
    
    // 是否加载 IPv6
    private final boolean enableIpv6;

    private IpDatabaseLoader(Builder builder) {
        this.ipv4HdfsPath = builder.ipv4HdfsPath;
        this.ipv6HdfsPath = builder.ipv6HdfsPath;
        this.isHdfsHA = builder.isHdfsHA;
        this.hdfsNameservice = builder.hdfsNameservice;
        this.hdfsNamenodes = builder.hdfsNamenodes;
        this.enableHotReload = builder.enableHotReload;
        this.reloadIntervalSeconds = builder.reloadIntervalSeconds;
        this.enableIpv6 = builder.enableIpv6;
    }

    /**
     * 初始化数据库
     */
    public void init() throws Exception {
        // 加载 IPv4 数据库
        loadIpv4Database();
        
        // 加载 IPv6 数据库
        if (enableIpv6 && ipv6HdfsPath != null && !ipv6HdfsPath.isEmpty()) {
            loadIpv6Database();
        }

        // 启动热更新
        if (enableHotReload) {
            startHotReload();
        }

        LOG.info("IP数据库初始化完成: IPv4={}, IPv6={}", 
                ipv4DatabaseRef.get() != null,
                ipv6DatabaseRef.get() != null);
    }

    /**
     * 查询IP地址
     */
    public String[] find(String ip) {
        if (ip == null || ip.isEmpty()) {
            return new String[]{"", "", ""};
        }

        try {
            InetAddress addr = InetAddress.getByName(ip);
            return find(addr);
        } catch (Exception e) {
            LOG.debug("IP解析失败: {}", ip);
            return new String[]{"", "", ""};
        }
    }

    /**
     * 查询IP地址
     */
    public String[] find(InetAddress ipAddress) {
        if (ipAddress == null) {
            return new String[]{"", "", ""};
        }

        try {
            // IPv6 地址
            if (ipAddress instanceof java.net.Inet6Address) {
                AwdbReader ipv6Db = ipv6DatabaseRef.get();
                if (ipv6Db != null) {
                    return ipv6Db.get(ipAddress);
                } else {
                    LOG.debug("IPv6数据库未加载: {}", ipAddress.getHostAddress());
                    return new String[]{"", "", ""};
                }
            }
            
            // IPv4 地址
            IpDatabase ipv4Db = ipv4DatabaseRef.get();
            if (ipv4Db != null) {
                return ipv4Db.find(ipAddress.getHostAddress());
            } else {
                LOG.warn("IPv4数据库未加载");
                return new String[]{"", "", ""};
            }
        } catch (Exception e) {
            LOG.error("IP查询异常: {}", ipAddress, e);
            return new String[]{"", "", ""};
        }
    }

    // ========== 私有方法 ==========

    private void loadIpv4Database() throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus fileStatus = getRecentFile(fs, ipv4HdfsPath);
        
        if (fileStatus == null) {
            throw new IOException("IPv4 数据文件不存在: " + ipv4HdfsPath);
        }

        byte[] data = readFileToBytes(fs, fileStatus);
        IpDatabase database = new IpDatabase(data);
        ipv4DatabaseRef.set(database);
        ipv4LastModifyTime = fileStatus.getModificationTime();
        
        LOG.info("IPv4数据库加载完成: path={}, size={}bytes", 
                fileStatus.getPath(), data.length);
    }

    private void loadIpv6Database() throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus fileStatus = getRecentFile(fs, ipv6HdfsPath);
        
        if (fileStatus == null) {
            LOG.warn("IPv6 数据文件不存在: {}", ipv6HdfsPath);
            return;
        }

        byte[] data = readFileToBytes(fs, fileStatus);
        AwdbReader database = new AwdbReader(data);
        ipv6DatabaseRef.set(database);
        ipv6LastModifyTime = fileStatus.getModificationTime();
        
        LOG.info("IPv6数据库(AWDB)加载完成: path={}, size={}bytes", 
                fileStatus.getPath(), data.length);
    }

    private FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        
        if (isHdfsHA && hdfsNameservice != null) {
            conf.set("fs.defaultFS", "hdfs://" + hdfsNameservice);
            conf.set("dfs.nameservices", hdfsNameservice);
            if (hdfsNamenodes != null) {
                conf.set("dfs.ha.namenodes." + hdfsNameservice, hdfsNamenodes);
            }
            conf.set("dfs.client.failover.proxy.provider." + hdfsNameservice,
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        }

        return FileSystem.get(conf);
    }

    private FileStatus getRecentFile(FileSystem fs, String dirPath) throws IOException {
        Path path = new Path(dirPath);
        
        if (!fs.exists(path)) {
            return null;
        }
        
        if (fs.isFile(path)) {
            return fs.getFileStatus(path);
        }

        // 目录则找最新文件
        FileStatus[] files = fs.listStatus(path);
        FileStatus recent = null;
        for (FileStatus file : files) {
            if (file.isFile()) {
                if (recent == null || file.getModificationTime() > recent.getModificationTime()) {
                    recent = file;
                }
            }
        }
        return recent;
    }

    private byte[] readFileToBytes(FileSystem fs, FileStatus fileStatus) throws IOException {
        try (FSDataInputStream in = fs.open(fileStatus.getPath());
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[16 * 1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            return out.toByteArray();
        }
    }

    private void startHotReload() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ip-database-hot-reload");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndReload();
            } catch (Exception e) {
                LOG.error("IP数据库热更新检查失败", e);
            }
        }, reloadIntervalSeconds, reloadIntervalSeconds, TimeUnit.SECONDS);

        LOG.info("IP数据库热更新已启动, 检查间隔: {}秒", reloadIntervalSeconds);
    }

    private void checkAndReload() throws Exception {
        FileSystem fs = getFileSystem();
        
        // 检查 IPv4
        FileStatus ipv4Status = getRecentFile(fs, ipv4HdfsPath);
        if (ipv4Status != null && ipv4Status.getModificationTime() > ipv4LastModifyTime) {
            LOG.info("检测到IPv4数据库更新, 重新加载...");
            loadIpv4Database();
        }
        
        // 检查 IPv6
        if (enableIpv6 && ipv6HdfsPath != null) {
            FileStatus ipv6Status = getRecentFile(fs, ipv6HdfsPath);
            if (ipv6Status != null && ipv6Status.getModificationTime() > ipv6LastModifyTime) {
                LOG.info("检测到IPv6数据库更新, 重新加载...");
                loadIpv6Database();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        ipv4DatabaseRef.set(null);
        ipv6DatabaseRef.set(null);
        
        LOG.info("IP数据库已关闭");
    }

    /**
     * 检查 IPv6 是否可用
     */
    public boolean isIpv6Enabled() {
        return ipv6DatabaseRef.get() != null;
    }

    // ========== Builder ==========

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String ipv4HdfsPath;
        private String ipv6HdfsPath;
        private boolean isHdfsHA = false;
        private String hdfsNameservice;
        private String hdfsNamenodes;
        private boolean enableHotReload = false;
        private long reloadIntervalSeconds = 300;
        private boolean enableIpv6 = true;

        public Builder ipv4HdfsPath(String path) {
            this.ipv4HdfsPath = path;
            return this;
        }

        public Builder ipv6HdfsPath(String path) {
            this.ipv6HdfsPath = path;
            return this;
        }

        public Builder hdfsHA(boolean isHA, String nameservice, String namenodes) {
            this.isHdfsHA = isHA;
            this.hdfsNameservice = nameservice;
            this.hdfsNamenodes = namenodes;
            return this;
        }

        public Builder enableHotReload(boolean enable) {
            this.enableHotReload = enable;
            return this;
        }

        public Builder reloadIntervalSeconds(long seconds) {
            this.reloadIntervalSeconds = seconds;
            return this;
        }
        
        public Builder enableIpv6(boolean enable) {
            this.enableIpv6 = enable;
            return this;
        }

        public IpDatabaseLoader build() {
            if (ipv4HdfsPath == null || ipv4HdfsPath.isEmpty()) {
                throw new IllegalArgumentException("ipv4HdfsPath is required");
            }
            return new IpDatabaseLoader(this);
        }
    }
}
