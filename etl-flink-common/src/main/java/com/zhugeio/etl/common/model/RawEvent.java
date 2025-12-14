package com.zhugeio.etl.common.model;

import java.io.Serializable;

/**
 * 原始事件模型
 *
 * 用于 Flink 数据流处理的核心数据结构
 * 包含用户行为事件的所有字段
 */
public class RawEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    // ========== 基础字段 ==========
    private String eventId;              // 事件ID
    private String eventName;            // 事件名称: $pageview, $startup, custom_event
    private String eventType;            // 事件类型: track, page, profile_set
    private Long eventTime;              // 事件时间戳(毫秒)
    private Long serverTime;             // 服务端接收时间戳(毫秒)

    // ========== 用户标识 ==========
    private String userId;               // 用户ID (已注册用户)
    private String distinctId;           // 访客ID (匿名用户或已注册用户)
    private String anonymousId;          // 匿名ID
    private String sessionId;            // 会话ID

    // ========== 设备信息 ==========
    private String platform;             // 平台: iOS, Android, Web, WeChat
    private String deviceId;             // 设备ID
    private String brand;                // 设备品牌: Apple, Huawei, Xiaomi
    private String model;                // 设备型号: iPhone 14, MI 13
    private String manufacturer;         // 制造商
    private String deviceType;           // 设备类型: Mobile, Tablet, Desktop (由UA解析填充)

    // ========== 操作系统信息 (由UA解析填充) ==========
    private String osName;               // 操作系统: Android, iOS, Windows
    private String osVersion;            // 操作系统版本: 13, 16.0, 10

    // ========== 浏览器信息 (由UA解析填充) ==========
    private String browserName;          // 浏览器名称: Chrome, Safari
    private String browserVersion;       // 浏览器版本
    private String ua;                   // User-Agent原始字符串

    // ========== 应用信息 ==========
    private String appId;                // 应用ID / 项目ID
    private String appVersion;           // 应用版本
    private String appName;              // 应用名称
    private String sdkVersion;           // SDK版本

    // ========== 网络信息 ==========
    private String ip;                   // 用户IP地址
    private String country;              // 国家 (由IP解析填充)
    private String province;             // 省份 (由IP解析填充)
    private String city;                 // 城市 (由IP解析填充)
    private String isp;                  // 运营商 (由IP解析填充)
    private String networkType;          // 网络类型: WIFI, 4G, 5G
    private String carrier;              // 运营商: ChinaMobile, ChinaUnicom

    // ========== 页面信息 ==========
    private String url;                  // 当前页面URL
    private String urlPath;              // URL路径
    private String referrer;             // 来源页面URL
    private String referrerHost;         // 来源域名
    private String title;                // 页面标题

    // ========== 搜索引擎信息 (由关键字解析填充) ==========
    private String searchEngine;         // 搜索引擎: baidu, google, sogou
    private String searchKeyword;        // 搜索关键字

    // ========== UTM 参数 ==========
    private String utmSource;            // 广告来源
    private String utmMedium;            // 广告媒介
    private String utmCampaign;          // 广告活动
    private String utmContent;           // 广告内容
    private String utmTerm;              // 广告关键字

    // ========== 屏幕信息 ==========
    private Integer screenWidth;         // 屏幕宽度
    private Integer screenHeight;        // 屏幕高度

    // ========== 位置信息 ==========
    private Double latitude;             // 纬度
    private Double longitude;            // 经度

    // ========== 自定义属性 ==========
    private String properties;           // JSON格式的自定义属性

    // ========== 数据质量字段 ==========
    private String dataSource;           // 数据来源: kafka_topic_name
    private String partition;            // Kafka分区
    private Long offset;                 // Kafka偏移量
    private Long processTime;            // 处理时间戳

    // ========== 构造函数 ==========

    public RawEvent() {
    }

    public RawEvent(String eventId, String eventName, Long eventTime) {
        this.eventId = eventId;
        this.eventName = eventName;
        this.eventTime = eventTime;
    }

    // ========== Getters and Setters ==========

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public Long getServerTime() {
        return serverTime;
    }

    public void setServerTime(Long serverTime) {
        this.serverTime = serverTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDistinctId() {
        return distinctId;
    }

    public void setDistinctId(String distinctId) {
        this.distinctId = distinctId;
    }

    public String getAnonymousId() {
        return anonymousId;
    }

    public void setAnonymousId(String anonymousId) {
        this.anonymousId = anonymousId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getManufacturer() {
        return manufacturer;
    }

    public void setManufacturer(String manufacturer) {
        this.manufacturer = manufacturer;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    public String getUa() {
        return ua;
    }

    public void setUa(String ua) {
        this.ua = ua;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public void setAppVersion(String appVersion) {
        this.appVersion = appVersion;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getSdkVersion() {
        return sdkVersion;
    }

    public void setSdkVersion(String sdkVersion) {
        this.sdkVersion = sdkVersion;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getIsp() {
        return isp;
    }

    public void setIsp(String isp) {
        this.isp = isp;
    }

    public String getNetworkType() {
        return networkType;
    }

    public void setNetworkType(String networkType) {
        this.networkType = networkType;
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrlPath() {
        return urlPath;
    }

    public void setUrlPath(String urlPath) {
        this.urlPath = urlPath;
    }

    public String getReferrer() {
        return referrer;
    }

    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    public String getReferrerHost() {
        return referrerHost;
    }

    public void setReferrerHost(String referrerHost) {
        this.referrerHost = referrerHost;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSearchEngine() {
        return searchEngine;
    }

    public void setSearchEngine(String searchEngine) {
        this.searchEngine = searchEngine;
    }

    public String getSearchKeyword() {
        return searchKeyword;
    }

    public void setSearchKeyword(String searchKeyword) {
        this.searchKeyword = searchKeyword;
    }

    public String getUtmSource() {
        return utmSource;
    }

    public void setUtmSource(String utmSource) {
        this.utmSource = utmSource;
    }

    public String getUtmMedium() {
        return utmMedium;
    }

    public void setUtmMedium(String utmMedium) {
        this.utmMedium = utmMedium;
    }

    public String getUtmCampaign() {
        return utmCampaign;
    }

    public void setUtmCampaign(String utmCampaign) {
        this.utmCampaign = utmCampaign;
    }

    public String getUtmContent() {
        return utmContent;
    }

    public void setUtmContent(String utmContent) {
        this.utmContent = utmContent;
    }

    public String getUtmTerm() {
        return utmTerm;
    }

    public void setUtmTerm(String utmTerm) {
        this.utmTerm = utmTerm;
    }

    public Integer getScreenWidth() {
        return screenWidth;
    }

    public void setScreenWidth(Integer screenWidth) {
        this.screenWidth = screenWidth;
    }

    public Integer getScreenHeight() {
        return screenHeight;
    }

    public void setScreenHeight(Integer screenHeight) {
        this.screenHeight = screenHeight;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getProcessTime() {
        return processTime;
    }

    public void setProcessTime(Long processTime) {
        this.processTime = processTime;
    }

    // ========== 工具方法 ==========

    /**
     * 判断是否为Web平台
     */
    public boolean isWeb() {
        return "Web".equalsIgnoreCase(platform) ||
                "JS".equalsIgnoreCase(platform) ||
                "H5".equalsIgnoreCase(platform);
    }

    /**
     * 判断是否为移动端
     */
    public boolean isMobile() {
        return "iOS".equalsIgnoreCase(platform) ||
                "Android".equalsIgnoreCase(platform);
    }

    /**
     * 判断是否为小程序
     */
    public boolean isMiniProgram() {
        return "WeChat".equalsIgnoreCase(platform) ||
                "WeChatMP".equalsIgnoreCase(platform) ||
                "Alipay".equalsIgnoreCase(platform);
    }

    /**
     * 获取主标识 (userId 优先,其次 distinctId)
     */
    public String getPrimaryId() {
        return userId != null && !userId.isEmpty() ? userId : distinctId;
    }

    /**
     * 判断是否为注册用户
     */
    public boolean isRegistered() {
        return userId != null && !userId.isEmpty();
    }

    /**
     * 判断是否来自搜索引擎
     */
    public boolean isFromSearchEngine() {
        return searchEngine != null &&
                !"unknown".equalsIgnoreCase(searchEngine) &&
                searchKeyword != null &&
                !searchKeyword.isEmpty();
    }

    /**
     * 判断是否有地理位置信息
     */
    public boolean hasLocation() {
        return (country != null && !country.isEmpty()) ||
                (province != null && !province.isEmpty()) ||
                (city != null && !city.isEmpty());
    }

    /**
     * 判断事件是否有效
     */
    public boolean isValid() {
        return eventId != null && !eventId.isEmpty() &&
                eventName != null && !eventName.isEmpty() &&
                eventTime != null && eventTime > 0 &&
                distinctId != null && !distinctId.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("RawEvent{eventId='%s', eventName='%s', userId='%s', " +
                        "distinctId='%s', platform='%s', ip='%s', country='%s', " +
                        "city='%s', os='%s', browser='%s', deviceType='%s'}",
                eventId, eventName, userId, distinctId, platform, ip,
                country, city, osName, browserName, deviceType);
    }

    /**
     * 创建一个副本
     */
    public RawEvent copy() {
        RawEvent copy = new RawEvent();

        // 基础字段
        copy.eventId = this.eventId;
        copy.eventName = this.eventName;
        copy.eventType = this.eventType;
        copy.eventTime = this.eventTime;
        copy.serverTime = this.serverTime;

        // 用户标识
        copy.userId = this.userId;
        copy.distinctId = this.distinctId;
        copy.anonymousId = this.anonymousId;
        copy.sessionId = this.sessionId;

        // 设备信息
        copy.platform = this.platform;
        copy.deviceId = this.deviceId;
        copy.brand = this.brand;
        copy.model = this.model;
        copy.manufacturer = this.manufacturer;
        copy.deviceType = this.deviceType;

        // OS和浏览器
        copy.osName = this.osName;
        copy.osVersion = this.osVersion;
        copy.browserName = this.browserName;
        copy.browserVersion = this.browserVersion;
        copy.ua = this.ua;

        // 应用信息
        copy.appId = this.appId;
        copy.appVersion = this.appVersion;
        copy.appName = this.appName;
        copy.sdkVersion = this.sdkVersion;

        // 网络和位置
        copy.ip = this.ip;
        copy.country = this.country;
        copy.province = this.province;
        copy.city = this.city;
        copy.isp = this.isp;
        copy.networkType = this.networkType;
        copy.carrier = this.carrier;

        // 页面信息
        copy.url = this.url;
        copy.urlPath = this.urlPath;
        copy.referrer = this.referrer;
        copy.referrerHost = this.referrerHost;
        copy.title = this.title;

        // 搜索引擎
        copy.searchEngine = this.searchEngine;
        copy.searchKeyword = this.searchKeyword;

        // UTM
        copy.utmSource = this.utmSource;
        copy.utmMedium = this.utmMedium;
        copy.utmCampaign = this.utmCampaign;
        copy.utmContent = this.utmContent;
        copy.utmTerm = this.utmTerm;

        // 屏幕和位置
        copy.screenWidth = this.screenWidth;
        copy.screenHeight = this.screenHeight;
        copy.latitude = this.latitude;
        copy.longitude = this.longitude;

        // 其他
        copy.properties = this.properties;
        copy.dataSource = this.dataSource;
        copy.partition = this.partition;
        copy.offset = this.offset;
        copy.processTime = this.processTime;

        return copy;
    }
}