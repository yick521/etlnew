package com.zhugeio.etl.common.model.output;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * 用户属性表行数据 (b_user_property_{appId})
 * 
 * 支持两种模式:
 * 1. 普通模式 (8列):
 *    zg_id, property_id, user_id, property_name, property_data_type, property_value, platform, last_update_date
 * 
 * 2. CDP 模式 (8列，列顺序不同):
 *    zg_id, property_id, property_value, user_id, property_name, property_data_type, platform, last_update_date
 */
public class UserPropertyRow implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    // NULL 值
    public static final String NULL_VALUE = "\\N";
    
    // 应用ID
    private Integer appId;
    
    // 是否是 CDP 模式
    private boolean cdpMode;
    
    // 字段值
    private String zgId;
    private String propertyId;
    private String userId;
    private String propertyName;
    private String propertyDataType;
    private String propertyValue;
    private Integer platform;
    private Long lastUpdateDate;
    
    public UserPropertyRow() {
        this.cdpMode = false;
    }
    
    public UserPropertyRow(Integer appId, boolean cdpMode) {
        this.appId = appId;
        this.cdpMode = cdpMode;
    }
    
    // Getters and Setters
    
    public Integer getAppId() {
        return appId;
    }
    
    public void setAppId(Integer appId) {
        this.appId = appId;
    }
    
    public boolean isCdpMode() {
        return cdpMode;
    }
    
    public void setCdpMode(boolean cdpMode) {
        this.cdpMode = cdpMode;
    }
    
    public String getZgId() {
        return zgId;
    }
    
    public void setZgId(String zgId) {
        this.zgId = zgId;
    }
    
    public String getPropertyId() {
        return propertyId;
    }
    
    public void setPropertyId(String propertyId) {
        this.propertyId = propertyId;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getPropertyName() {
        return propertyName;
    }
    
    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }
    
    public String getPropertyDataType() {
        return propertyDataType;
    }
    
    public void setPropertyDataType(String propertyDataType) {
        this.propertyDataType = propertyDataType;
    }
    
    public String getPropertyValue() {
        return propertyValue;
    }
    
    public void setPropertyValue(String propertyValue) {
        this.propertyValue = propertyValue;
    }
    
    public Integer getPlatform() {
        return platform;
    }
    
    public void setPlatform(Integer platform) {
        this.platform = platform;
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
        return "b_user_property_" + appId;
    }
    
    /**
     * 获取去重键
     * 格式: zg_id + property_id
     */
    public String getDedupeKey() {
        return nullSafe(zgId) + "_" + nullSafe(propertyId);
    }
    
    /**
     * 转换为 TSV 格式
     * 根据 cdpMode 决定列顺序
     */
    public String toTsv() {
        StringJoiner joiner = new StringJoiner("\t");
        
        if (cdpMode) {
            // CDP 模式: zg_id, property_id, property_value, user_id, property_name, property_data_type, platform, last_update_date
            joiner.add(nullSafe(zgId));
            joiner.add(nullSafe(propertyId));
            joiner.add(nullSafe(propertyValue));
            joiner.add(nullSafe(userId));
            joiner.add(nullSafe(propertyName));
            joiner.add(nullSafe(propertyDataType));
            joiner.add(platform != null ? String.valueOf(platform) : NULL_VALUE);
            joiner.add(lastUpdateDate != null ? String.valueOf(lastUpdateDate) : NULL_VALUE);
        } else {
            // 普通模式: zg_id, property_id, user_id, property_name, property_data_type, property_value, platform, last_update_date
            joiner.add(nullSafe(zgId));
            joiner.add(nullSafe(propertyId));
            joiner.add(nullSafe(userId));
            joiner.add(nullSafe(propertyName));
            joiner.add(nullSafe(propertyDataType));
            joiner.add(nullSafe(propertyValue));
            joiner.add(platform != null ? String.valueOf(platform) : NULL_VALUE);
            joiner.add(lastUpdateDate != null ? String.valueOf(lastUpdateDate) : NULL_VALUE);
        }
        
        return joiner.toString();
    }
    
    private String nullSafe(String value) {
        return value != null && !value.isEmpty() ? value : NULL_VALUE;
    }
    
    @Override
    public String toString() {
        return "UserPropertyRow{" +
                "appId=" + appId +
                ", cdpMode=" + cdpMode +
                ", zgId='" + zgId + '\'' +
                ", propertyId='" + propertyId + '\'' +
                ", propertyName='" + propertyName + '\'' +
                ", propertyValue='" + propertyValue + '\'' +
                '}';
    }
}
