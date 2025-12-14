package com.zhugeio.etl.pipeline.transfer;

import com.zhugeio.etl.common.model.output.UserRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * 用户映射转换器
 * 
 * 对应 Scala: UserTransfer
 * 
 * 处理的事件类型: zgid
 */
public class UserTransfer implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(UserTransfer.class);
    
    private static final String NULL_VALUE = "\\N";
    
    // 线程安全的日期格式化
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    
    /**
     * 转换用户映射数据 (从 Map)
     * 
     * @param appId 应用ID
     * @param platform 平台
     * @param pr properties Map
     * @return UserRow，如果数据无效返回 null
     */
    public UserRow transferFromMap(Integer appId, Integer platform, Map<String, Object> pr) {
        if (pr == null || appId == null) {
            return null;
        }
        
        // 获取必要字段
        String zgDid = getStringValue(pr, "$zg_did");
        String zgZgid = getStringValue(pr, "$zg_zgid");
        
        // 校验必要字段
        if (isNullOrEmpty(zgDid) || isNullOrEmpty(zgZgid)) {
            return null;
        }
        
        // 检查时间字段
        Long ct = getLongValue(pr, "$ct");
        Integer tz = getIntValue(pr, "$tz");
        
        if (ct == null || tz == null) {
            return null;
        }
        
        String time = timestampToDateString(ct, tz);
        if (isNullOrEmpty(time)) {
            return null;
        }
        
        // 创建行对象
        UserRow row = new UserRow(appId);
        row.setDeviceId(zgDid);
        row.setZgId(zgZgid);
        row.setUserId(getStringValue(pr, "$zg_uid"));
        row.setBeginDate(Timestamp.valueOf(time).getTime() / 1000);
        row.setPlatform(platform);
        
        return row;
    }
    
    /**
     * 时间戳转日期字符串
     */
    private String timestampToDateString(Long ct, Integer tz) {
        if (ct == null || tz == null) {
            return NULL_VALUE;
        }
        
        // 检查时区范围
        if (Math.abs(tz) > 48 * 3600 * 1000) {
            return NULL_VALUE;
        }
        
        try {
            return DATE_FORMAT.get().format(ct);
        } catch (Exception e) {
            return NULL_VALUE;
        }
    }
    
    private String getStringValue(Map<String, Object> map, String key) {
        if (map == null || key == null) {
            return NULL_VALUE;
        }
        Object value = map.get(key);
        return value != null ? String.valueOf(value) : NULL_VALUE;
    }
    
    private Long getLongValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).longValue();
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    private Integer getIntValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).intValue();
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    private boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty() || NULL_VALUE.equals(value);
    }
}
