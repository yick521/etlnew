package com.zhugeio.etl.common.model.ua;

import java.io.Serializable;

/**
 * User-Agent解析结果
 */
public class UserAgentInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    // 操作系统
    private String osName;          // 操作系统名称: Android, iOS, Windows, MacOS, Linux
    private String osVersion;       // 操作系统版本: 13, 16.0, 10
    private String osFamily;        // 操作系统家族: Android, iOS, Windows

    // 浏览器
    private String browserName;     // 浏览器名称: Chrome, Safari, Firefox
    private String browserVersion;  // 浏览器版本: 120.0.0.0
    private String browserFamily;   // 浏览器家族: Chrome, Safari

    // 设备
    private String deviceType;      // 设备类型: Mobile, Tablet, Desktop
    private String deviceBrand;     // 设备品牌: Apple, Samsung, Huawei
    private String deviceModel;     // 设备型号: iPhone 14, Galaxy S23

    // 原始UA
    private String userAgent;

    public UserAgentInfo() {
    }

    public UserAgentInfo(String userAgent) {
        this.userAgent = userAgent;
    }

    // Getters and Setters
    public String getOsName() { return osName; }
    public void setOsName(String osName) { this.osName = osName; }

    public String getOsVersion() { return osVersion; }
    public void setOsVersion(String osVersion) { this.osVersion = osVersion; }

    public String getOsFamily() { return osFamily; }
    public void setOsFamily(String osFamily) { this.osFamily = osFamily; }

    public String getBrowserName() { return browserName; }
    public void setBrowserName(String browserName) { this.browserName = browserName; }

    public String getBrowserVersion() { return browserVersion; }
    public void setBrowserVersion(String browserVersion) { this.browserVersion = browserVersion; }

    public String getBrowserFamily() { return browserFamily; }
    public void setBrowserFamily(String browserFamily) { this.browserFamily = browserFamily; }

    public String getDeviceType() { return deviceType; }
    public void setDeviceType(String deviceType) { this.deviceType = deviceType; }

    public String getDeviceBrand() { return deviceBrand; }
    public void setDeviceBrand(String deviceBrand) { this.deviceBrand = deviceBrand; }

    public String getDeviceModel() { return deviceModel; }
    public void setDeviceModel(String deviceModel) { this.deviceModel = deviceModel; }

    public String getUserAgent() { return userAgent; }
    public void setUserAgent(String userAgent) { this.userAgent = userAgent; }

    /**
     * 判断是否为移动设备
     */
    public boolean isMobile() {
        return "Mobile".equalsIgnoreCase(deviceType) || "Tablet".equalsIgnoreCase(deviceType);
    }

    /**
     * 判断是否为Android
     */
    public boolean isAndroid() {
        return "Android".equalsIgnoreCase(osName);
    }

    /**
     * 判断是否为iOS
     */
    public boolean isIOS() {
        return "iOS".equalsIgnoreCase(osName) || "iPhone".equalsIgnoreCase(osName);
    }

    @Override
    public String toString() {
        return String.format("UserAgentInfo{os='%s %s', browser='%s %s', device='%s %s %s'}",
                osName, osVersion, browserName, browserVersion, deviceType, deviceBrand, deviceModel);
    }
}
