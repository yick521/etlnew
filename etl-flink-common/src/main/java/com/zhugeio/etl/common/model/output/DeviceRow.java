package com.zhugeio.etl.common.model.output;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * 设备表行数据 (b_device_{appId})
 * 
 * 表结构 (22列):
 * 0: device_id - 设备ID
 * 1: device_md5 - 设备MD5
 * 2: platform - 平台
 * 3: device_type - 设备类型
 * 4: l - 水平像素
 * 5: h - 垂直像素
 * 6: device_brand - 设备品牌
 * 7: device_model - 设备型号
 * 8: resolution - 分辨率
 * 9: phone - 手机号
 * 10: imei - IMEI
 * 11: mac - MAC地址
 * 12: is_prison_break - 是否越狱
 * 13: is_crack - 是否破解
 * 14: language - 语言
 * 15: timezone - 时区
 * 16: attr1 - 扩展属性1 (zgsee标志)
 * 17: attr2 - 扩展属性2
 * 18: attr3 - 扩展属性3
 * 19: attr4 - 扩展属性4
 * 20: attr5 - 扩展属性5
 * 21: last_update_date - 最后更新时间 (秒)
 */
public class DeviceRow implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    public static final String NULL_VALUE = "\\N";
    
    private Integer appId;
    
    // 基础字段
    private String deviceId;
    private String deviceMd5;
    private Integer platform;
    private String deviceType;
    private String horizontalPixel;  // l
    private String verticalPixel;    // h
    private String deviceBrand;
    private String deviceModel;
    private String resolution;
    private String phone;
    private String imei;
    private String mac;
    private String isPrisonBreak;
    private String isCrack;
    private String language;
    private String timezone;
    
    // 扩展字段
    private String attr1;
    private String attr2;
    private String attr3;
    private String attr4;
    private String attr5;
    
    // 时间字段
    private Long lastUpdateDate;
    
    public DeviceRow() {
    }
    
    public DeviceRow(Integer appId) {
        this.appId = appId;
    }
    
    // Getters and Setters
    
    public Integer getAppId() {
        return appId;
    }
    
    public void setAppId(Integer appId) {
        this.appId = appId;
    }
    
    public String getDeviceId() {
        return deviceId;
    }
    
    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }
    
    public String getDeviceMd5() {
        return deviceMd5;
    }
    
    public void setDeviceMd5(String deviceMd5) {
        this.deviceMd5 = deviceMd5;
    }
    
    public Integer getPlatform() {
        return platform;
    }
    
    public void setPlatform(Integer platform) {
        this.platform = platform;
    }
    
    public String getDeviceType() {
        return deviceType;
    }
    
    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }
    
    public String getHorizontalPixel() {
        return horizontalPixel;
    }
    
    public void setHorizontalPixel(String horizontalPixel) {
        this.horizontalPixel = horizontalPixel;
    }
    
    public String getVerticalPixel() {
        return verticalPixel;
    }
    
    public void setVerticalPixel(String verticalPixel) {
        this.verticalPixel = verticalPixel;
    }
    
    public String getDeviceBrand() {
        return deviceBrand;
    }
    
    public void setDeviceBrand(String deviceBrand) {
        this.deviceBrand = deviceBrand;
    }
    
    public String getDeviceModel() {
        return deviceModel;
    }
    
    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }
    
    public String getResolution() {
        return resolution;
    }
    
    public void setResolution(String resolution) {
        this.resolution = resolution;
    }
    
    public String getPhone() {
        return phone;
    }
    
    public void setPhone(String phone) {
        this.phone = phone;
    }
    
    public String getImei() {
        return imei;
    }
    
    public void setImei(String imei) {
        this.imei = imei;
    }
    
    public String getMac() {
        return mac;
    }
    
    public void setMac(String mac) {
        this.mac = mac;
    }
    
    public String getIsPrisonBreak() {
        return isPrisonBreak;
    }
    
    public void setIsPrisonBreak(String isPrisonBreak) {
        this.isPrisonBreak = isPrisonBreak;
    }
    
    public String getIsCrack() {
        return isCrack;
    }
    
    public void setIsCrack(String isCrack) {
        this.isCrack = isCrack;
    }
    
    public String getLanguage() {
        return language;
    }
    
    public void setLanguage(String language) {
        this.language = language;
    }
    
    public String getTimezone() {
        return timezone;
    }
    
    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
    
    public String getAttr1() {
        return attr1;
    }
    
    public void setAttr1(String attr1) {
        this.attr1 = attr1;
    }
    
    public String getAttr2() {
        return attr2;
    }
    
    public void setAttr2(String attr2) {
        this.attr2 = attr2;
    }
    
    public String getAttr3() {
        return attr3;
    }
    
    public void setAttr3(String attr3) {
        this.attr3 = attr3;
    }
    
    public String getAttr4() {
        return attr4;
    }
    
    public void setAttr4(String attr4) {
        this.attr4 = attr4;
    }
    
    public String getAttr5() {
        return attr5;
    }
    
    public void setAttr5(String attr5) {
        this.attr5 = attr5;
    }
    
    public Long getLastUpdateDate() {
        return lastUpdateDate;
    }
    
    public void setLastUpdateDate(Long lastUpdateDate) {
        this.lastUpdateDate = lastUpdateDate;
    }
    
    /**
     * 获取表名
     */
    public String getTableName() {
        return "b_device_" + appId;
    }
    
    /**
     * 获取去重键
     * 格式: device_id
     */
    public String getDedupeKey() {
        return nullSafe(deviceId);
    }
    
    /**
     * 判断 device_model 是否为空 (用于区分空设备和非空设备)
     */
    public boolean isDeviceModelEmpty() {
        return "zhuge.io".equals(deviceModel) || isNullOrEmpty(deviceModel);
    }
    
    /**
     * 转换为 TSV 格式
     */
    public String toTsv() {
        StringJoiner joiner = new StringJoiner("\t");
        joiner.add(nullSafe(deviceId));
        joiner.add(nullSafe(deviceMd5));
        joiner.add(platform != null ? String.valueOf(platform) : NULL_VALUE);
        joiner.add(nullSafe(deviceType));
        joiner.add(nullSafe(horizontalPixel));
        joiner.add(nullSafe(verticalPixel));
        joiner.add(nullSafe(deviceBrand));
        joiner.add(nullSafe(deviceModel));
        joiner.add(nullSafe(resolution));
        joiner.add(nullSafe(phone));
        joiner.add(nullSafe(imei));
        joiner.add(nullSafe(mac));
        joiner.add(nullSafe(isPrisonBreak));
        joiner.add(nullSafe(isCrack));
        joiner.add(nullSafe(language));
        joiner.add(nullSafe(timezone));
        joiner.add(nullSafe(attr1));
        joiner.add(nullSafe(attr2));
        joiner.add(nullSafe(attr3));
        joiner.add(nullSafe(attr4));
        joiner.add(nullSafe(attr5));
        joiner.add(lastUpdateDate != null ? String.valueOf(lastUpdateDate) : NULL_VALUE);
        return joiner.toString();
    }
    
    private String nullSafe(String value) {
        return value != null && !value.isEmpty() ? value : NULL_VALUE;
    }
    
    private boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty() || NULL_VALUE.equals(value);
    }
    
    @Override
    public String toString() {
        return "DeviceRow{" +
                "appId=" + appId +
                ", deviceId='" + deviceId + '\'' +
                ", deviceBrand='" + deviceBrand + '\'' +
                ", deviceModel='" + deviceModel + '\'' +
                '}';
    }
}
