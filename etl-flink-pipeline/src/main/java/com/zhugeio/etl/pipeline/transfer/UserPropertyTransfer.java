package com.zhugeio.etl.pipeline.transfer;

import com.zhugeio.etl.common.model.output.UserPropertyRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 用户属性转换器
 * 
 * 对应 Scala: UserPropertyTransfer
 * 
 * 处理的事件类型: usr
 */
public class UserPropertyTransfer implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(UserPropertyTransfer.class);
    
    private static final String NULL_VALUE = "\\N";
    private static final String APP_USER_ID_PROPERTY_ID = "-1";
    private static final String APP_USER_ID_PROPERTY_NAME = "app_user_id";
    private static final String APP_USER_ID_DATA_TYPE = "string";
    
    private Set<Integer> cdpAppIds;
    
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    
    public UserPropertyTransfer() {}
    
    public void setCdpAppIds(Set<Integer> cdpAppIds) {
        this.cdpAppIds = cdpAppIds;
    }
    
    /**
     * 转换用户属性数据 (从 Map)
     */
    public List<UserPropertyRow> transferFromMap(Integer appId, Integer platform, Map<String, Object> pr) {
        List<UserPropertyRow> result = new ArrayList<>();
        
        if (pr == null || appId == null) {
            return result;
        }
        
        String zgId = getStringValue(pr, "$zg_zgid");
        String userId = getStringValue(pr, "$zg_uid");
        
        if (isNullOrEmpty(zgId) || isNullOrEmpty(userId)) {
            return result;
        }
        
        Long ct = getLongValue(pr, "$ct");
        Integer tz = getIntValue(pr, "$tz");
        
        if (ct == null || tz == null) {
            return result;
        }
        
        String time = timestampToDateString(ct, tz);
        if (isNullOrEmpty(time)) {
            return result;
        }
        
        long timestamp = Timestamp.valueOf(time).getTime() / 1000;
        boolean cdpMode = cdpAppIds != null && cdpAppIds.contains(appId);
        
        // 处理自定义属性 (以 "_" 开头)
        for (String key : pr.keySet()) {
            if (key.startsWith("_")) {
                UserPropertyRow row = processCustomPropertyFromMap(appId, platform, pr, key, 
                        zgId, userId, timestamp, cdpMode);
                if (row != null) {
                    result.add(row);
                }
            }
        }
        
        // 处理 app_user_id ($cuid)
        String cuid = getStringValue(pr, "$cuid");
        if (!isNullOrEmpty(cuid)) {
            UserPropertyRow row = processAppUserId(appId, platform, zgId, userId, 
                    cuid, timestamp, cdpMode);
            result.add(row);
        }
        
        return result;
    }
    
    private UserPropertyRow processCustomPropertyFromMap(Integer appId, Integer platform, Map<String, Object> pr,
                                                   String propKey, String zgId, String userId, 
                                                   long timestamp, boolean cdpMode) {
        String propIdKey = "$zg_upid#" + propKey;
        String propId = getStringValue(pr, propIdKey);
        
        if (isNullOrEmpty(propId)) {
            return null;
        }
        
        String propName = ensureLength(propKey.substring(1), 256);
        String propValue = ensureLength(getStringValue(pr, propKey), 256);
        
        String propTypeKey = "$zg_uptp#" + propKey;
        String propType = ensureLength(getStringValue(pr, propTypeKey), 256);
        
        UserPropertyRow row = new UserPropertyRow(appId, cdpMode);
        row.setZgId(zgId);
        row.setPropertyId(propId);
        row.setUserId(userId);
        row.setPropertyName(propName);
        row.setPropertyDataType(propType);
        row.setPropertyValue(propValue);
        row.setPlatform(platform);
        row.setLastUpdateDate(timestamp);
        
        return row;
    }
    
    private UserPropertyRow processAppUserId(Integer appId, Integer platform, String zgId, 
                                              String userId, String cuid, long timestamp, 
                                              boolean cdpMode) {
        UserPropertyRow row = new UserPropertyRow(appId, cdpMode);
        row.setZgId(zgId);
        row.setPropertyId(APP_USER_ID_PROPERTY_ID);
        row.setUserId(userId);
        row.setPropertyName(APP_USER_ID_PROPERTY_NAME);
        row.setPropertyDataType(APP_USER_ID_DATA_TYPE);
        row.setPropertyValue(ensureLength(cuid, 256));
        row.setPlatform(platform);
        row.setLastUpdateDate(timestamp);
        return row;
    }
    
    private String timestampToDateString(Long ct, Integer tz) {
        if (ct == null || tz == null) return NULL_VALUE;
        if (Math.abs(tz) > 48 * 3600 * 1000) return NULL_VALUE;
        try { return DATE_FORMAT.get().format(ct); } 
        catch (Exception e) { return NULL_VALUE; }
    }
    
    private String getStringValue(Map<String, Object> map, String key) {
        if (map == null || key == null) return NULL_VALUE;
        Object value = map.get(key);
        return value != null ? String.valueOf(value) : NULL_VALUE;
    }
    
    private Long getLongValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).longValue();
        try { return Long.parseLong(String.valueOf(value)); } 
        catch (NumberFormatException e) { return null; }
    }
    
    private Integer getIntValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).intValue();
        try { return Integer.parseInt(String.valueOf(value)); } 
        catch (NumberFormatException e) { return null; }
    }
    
    private boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty() || NULL_VALUE.equals(value);
    }
    
    private String ensureLength(String value, int maxLength) {
        if (isNullOrEmpty(value)) return NULL_VALUE;
        value = value.replaceAll("[\t\n\r\"\\\\\u0000]", " ").trim();
        return value.length() > maxLength ? value.substring(0, maxLength) : value;
    }
}
