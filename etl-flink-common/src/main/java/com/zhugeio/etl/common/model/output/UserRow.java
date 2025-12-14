package com.zhugeio.etl.common.model.output;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * 用户映射表行数据 (b_user_{appId})
 * 
 * 表结构 (5列):
 * - device_id: 设备ID
 * - zg_id: 诸葛ID
 * - user_id: 注册用户ID
 * - begin_date: 开始时间戳 (秒)
 * - platform: 平台
 */
public class UserRow implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    public static final String NULL_VALUE = "\\N";
    
    private Integer appId;
    private String deviceId;
    private String zgId;
    private String userId;
    private Long beginDate;
    private Integer platform;
    
    public UserRow() {
    }
    
    public UserRow(Integer appId) {
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
    
    public String getZgId() {
        return zgId;
    }
    
    public void setZgId(String zgId) {
        this.zgId = zgId;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public Long getBeginDate() {
        return beginDate;
    }
    
    public void setBeginDate(Long beginDate) {
        this.beginDate = beginDate;
    }
    
    public Integer getPlatform() {
        return platform;
    }
    
    public void setPlatform(Integer platform) {
        this.platform = platform;
    }
    
    /**
     * 获取表名
     */
    public String getTableName() {
        return "b_user_" + appId;
    }
    
    /**
     * 获取去重键
     * 格式: device_id + zg_id
     */
    public String getDedupeKey() {
        return nullSafe(deviceId) + "_" + nullSafe(zgId);
    }
    
    /**
     * 转换为 TSV 格式
     */
    public String toTsv() {
        StringJoiner joiner = new StringJoiner("\t");
        joiner.add(nullSafe(deviceId));
        joiner.add(nullSafe(zgId));
        joiner.add(nullSafe(userId));
        joiner.add(beginDate != null ? String.valueOf(beginDate) : NULL_VALUE);
        joiner.add(platform != null ? String.valueOf(platform) : NULL_VALUE);
        return joiner.toString();
    }
    
    private String nullSafe(String value) {
        return value != null && !value.isEmpty() ? value : NULL_VALUE;
    }
    
    @Override
    public String toString() {
        return "UserRow{" +
                "appId=" + appId +
                ", deviceId='" + deviceId + '\'' +
                ", zgId='" + zgId + '\'' +
                ", userId='" + userId + '\'' +
                '}';
    }
}
