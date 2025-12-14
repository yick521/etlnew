package com.zhugeio.etl.pipeline.transfer;

import com.zhugeio.etl.common.model.output.EventAttrRow;
import com.zhugeio.etl.pipeline.service.EventAttrColumnService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 事件属性转换器
 * 
 * 处理的事件类型: evt, vtl, mkt, ss, se, abp
 */
public class EventAttrTransfer implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EventAttrTransfer.class);
    
    private static final String NULL_VALUE = "\\N";
    
    private static final String WWW_BAIDU_COM = ".baidu.com";
    private static final String WWW_SOGOU_COM = ".sogou.com";
    private static final String CN_BING_COM = ".bing.com";
    private static final String WWW_SO_COM = ".so.com";
    private static final String WWW_GOOGLE_COM = ".google.com";
    private static final String WWW_GOOGLE_CO = ".google.co";
    private static final String M_SM_CN = "m.sm.cn";
    
    private static final Set<String> CUSTOM_PROPERTY_DT = new HashSet<>(Arrays.asList("evt", "mkt", "abp"));
    
    private final int expireSubDays;
    private final int expireAddDays;
    private final EventAttrColumnService columnService;
    private final int eventAttrLengthLimit;
    
    private Set<String> mktAttrs;
    private Set<String> abpAttrs;
    
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    private static final ThreadLocal<SimpleDateFormat> DAY_FORMAT = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd"));
    private static final ThreadLocal<SimpleDateFormat> YEAR_WEEK_FORMAT = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("YYYYww"));
    
    public EventAttrTransfer(EventAttrColumnService columnService, int expireSubDays, int expireAddDays, int eventAttrLengthLimit) {
        this.columnService = columnService;
        this.expireSubDays = expireSubDays;
        this.expireAddDays = expireAddDays;
        this.eventAttrLengthLimit = eventAttrLengthLimit;
        
        this.mktAttrs = new HashSet<>(Arrays.asList(
                "ct", "tz", "zg_zgid", "zg_did", "zg_eid", "zg_sid", "zg_uid", "eid", "uuid"
        ));
        this.abpAttrs = new HashSet<>(Arrays.asList(
                "ct", "tz", "zg_zgid", "zg_did", "zg_eid", "zg_sid", "zg_uid", "eid", "uuid"
        ));
    }
    
    public void setMktAttrs(Set<String> mktAttrs) { this.mktAttrs = mktAttrs; }
    public void setAbpAttrs(Set<String> abpAttrs) { this.abpAttrs = abpAttrs; }
    
    /**
     * 转换事件数据 (从 Map)
     */
    public EventAttrRow transferFromMap(Integer appId, Integer platform, String dt, Map<String, Object> pr,
                                  String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                  String business, Map<String, String> eqidKeywords) {
        
        String zgId = getStringValue(pr, "$zg_zgid");
        String zgEid = getStringValue(pr, "$zg_eid");
        String zgDid = getStringValue(pr, "$zg_did");
        
        if (isNullOrEmpty(zgId) || isNullOrEmpty(zgEid) || isNullOrEmpty(zgDid)) {
            return null;
        }
        
        Long ct = getLongValue(pr, "$ct");
        Integer tz = getIntValue(pr, "$tz");
        
        if (ct == null || tz == null) {
            return null;
        }
        
        String realtime = timestampToDateString(ct, tz);
        if (isNullOrEmpty(realtime) || isExpiredTime(realtime)) {
            return null;
        }
        
        Map<String, String> timeComponents = getTimeComponents(ct);
        if (timeComponents.isEmpty()) {
            return null;
        }
        
        EventAttrRow row = new EventAttrRow(appId);
        
        // 填充基础字段
        fillBasicFieldsFromMap(row, pr, platform, dt, ip, ipResult, ua, uaResult, 
                        business, eqidKeywords, realtime, timeComponents);
        
        // 填充自定义属性
        if (CUSTOM_PROPERTY_DT.contains(dt)) {
            fillCustomPropertiesFromMap(row, pr, dt, zgEid);
        }
        
        row.setEid(zgEid);
        row.setYw(getYearWeek(realtime));
        
        return row;
    }
    
    private void fillBasicFieldsFromMap(EventAttrRow row, Map<String, Object> pr, Integer platform, String dt,
                                  String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                  String business, Map<String, String> eqidKeywords,
                                  String realtime, Map<String, String> timeComponents) {
        
        // 列0-3: 核心ID
        row.setZgId(getStringValue(pr, "$zg_zgid"));
        row.setSessionId(getStringValue(pr, "$zg_sid"));
        row.setUuid(ensureLength(getStringValue(pr, "$uuid"), 256));
        row.setEventIdColumn(getStringValue(pr, "$zg_eid"));
        
        // 列4-6: 时间字段
        row.setBeginDayId(timeComponents.get("day"));
        row.setBeginDate(String.valueOf(Timestamp.valueOf(realtime).getTime() / 1000));
        row.setBeginTimeId(timeComponents.get("hour") + "0" + timeComponents.get("mill"));
        
        // 列7-9: 用户/设备/事件
        row.setDeviceId(getStringValue(pr, "$zg_did"));
        row.setUserId(getStringValue(pr, "$zg_uid"));
        row.setEventName(ensureLength(getEventNameFromMap(pr), 256));
        
        // 列10-13: 平台/网络
        row.setPlatform(platform);
        row.setNetwork(ensureNetwork(getStringValue(pr, "$net")));
        row.setMccmnc(ensureIntLength(getStringValue(pr, "$cr"), 256));
        row.setUseragent(ensureLength(ua, 256));
        
        // 列14-16: URL
        String website = ensureLength(getStringValue(pr, "$referrer_domain"), 1088);
        String currentUrl = ensureLength(getStringValue(pr, "$url"), 1088);
        String referrerUrl = ensureLength(getStringValue(pr, "$ref"), 1088);
        
        row.setWebsite(website);
        row.setCurrentUrl(currentUrl);
        row.setReferrerUrl(referrerUrl);
        
        // 列17-18: 渠道/版本
        row.setChannel(ensureLength(getStringValue(pr, "$cn"), 256));
        row.setAppVersion(ensureLength(getStringValue(pr, "$vn"), 256));
        
        // 列19-20: IP
        Long ipLong = ipToLong(ip);
        row.setIp(ipLong);
        row.setIpStr(ensureLength(ip, 256));
        
        // 列21-23: 地理位置
        if (ipResult != null && ipResult.length >= 3) {
            row.setCountry(ensureLength(ipResult[0], 256));
            row.setArea(ensureLength(ipResult[1], 256));
            row.setCity(ensureLength(ipResult[2], 256));
        } else {
            row.setCountry(NULL_VALUE);
            row.setArea(NULL_VALUE);
            row.setCity(NULL_VALUE);
        }
        
        // 列24-27: 操作系统/浏览器
        if (uaResult != null) {
            row.setOs(ensureLength(uaResult.get("os"), 256));
            row.setOv(ensureLength(uaResult.get("os_version"), 256));
            row.setBs(ensureLength(uaResult.get("browser"), 256));
            row.setBv(ensureLength(uaResult.get("browser_version"), 256));
        } else {
            row.setOs(NULL_VALUE);
            row.setOv(NULL_VALUE);
            row.setBs(NULL_VALUE);
            row.setBv(NULL_VALUE);
        }
        
        // 列28-32: UTM (与Scala版本一致的推断逻辑)
        String source = getStringValue(pr, "$utm_source");
        String medium = getStringValue(pr, "$utm_medium");
        String campaign = getStringValue(pr, "$utm_campaign");
        String utmContent = getStringValue(pr, "$utm_content");
        String utmTerm = getStringValue(pr, "$utm_term");
        String eqid = getStringValue(pr, "$eqid");
        
        // 当所有UTM参数都为空时，根据来源网站推断
        if (isNullOrEmpty(source) && isNullOrEmpty(medium) && isNullOrEmpty(campaign) 
                && isNullOrEmpty(utmContent) && isNullOrEmpty(utmTerm)) {
            
            if (isSearchEngine(website)) {
                // 搜索引擎流量
                medium = "搜索自然流量";
                source = website;
                if (isNullOrEmpty(eqid)) {
                    utmTerm = getUtmTermFromRef(utmTerm, referrerUrl);
                } else {
                    utmTerm = eqidKeywords != null ? eqidKeywords.getOrDefault(eqid, NULL_VALUE) : NULL_VALUE;
                }
            } else {
                if (isNullOrEmpty(referrerUrl)) {
                    medium = NULL_VALUE;
                    source = NULL_VALUE;
                } else {
                    // 引荐流量
                    medium = "引荐";
                    source = website;
                    if (isNullOrEmpty(utmTerm)) {
                        if (isNullOrEmpty(eqid)) {
                            utmTerm = getUtmTermFromRef(utmTerm, referrerUrl);
                        } else {
                            utmTerm = eqidKeywords != null ? eqidKeywords.getOrDefault(eqid, NULL_VALUE) : NULL_VALUE;
                        }
                    }
                }
            }
        } else {
            // 有UTM参数时，source 默认用 website
            source = ensureLength(isNullOrEmpty(source) ? website : source, 256);
        }
        
        row.setUtmSource(ensureLength(source, 256));
        row.setUtmMedium(ensureLength(medium, 256));
        row.setUtmCampaign(ensureLength(campaign, 256));
        row.setUtmContent(ensureLength(utmContent, 256));
        row.setUtmTerm(ensureLength(utmTerm, 256));
        
        // 列33-34: 持续时间/UTC
        row.setDuration(ensureIntRange(getStringValue(pr, "$dru"), 0, 86400000));
        row.setUtcDate(String.valueOf(System.currentTimeMillis()));
        
        // 列35-39: attr
        row.setAttr1(ensureLength(business, 256));
        row.setAttr2(ensureLength(getStringValue(pr, "$wxeid"), 256));
        row.setAttr3(ensureLength(getStringValue(pr, "$env_attr3"), 256));
        row.setAttr4(NULL_VALUE);
        row.setAttr5(getStringValue(pr, "$zg_zgid") + "_" + getStringValue(pr, "$zg_sid"));
    }
    
    private void fillCustomPropertiesFromMap(EventAttrRow row, Map<String, Object> pr, String dt, String zgEid) {
        Set<String> attrSet = getAttrSet(dt);
        
        for (String key : pr.keySet()) {
            boolean isCustomProp = false;
            
            // evt 类型: 以 "_" 开头的属性
            if ("evt".equals(dt) && key.startsWith("_")) {
                isCustomProp = true;
            } 
            // mkt/abp 类型: 不以 "$" 开头，且不在排除集合中的属性
            else if (("mkt".equals(dt) || "abp".equals(dt)) && !key.startsWith("$") && !attrSet.contains(key.replace("$", ""))) {
                isCustomProp = true;
            }
            
            if (isCustomProp) {
                // 使用 $zg_epid# 获取属性ID (与Scala一致)
                String propIdKey = "$zg_epid#" + key;
                String propId = getStringValue(pr, propIdKey);
                
                if (!isNullOrEmpty(propId)) {
                    // 获取列索引 (1-100)
                    int colIndex = columnService.getColumnIndex(zgEid, propId);
                    if (colIndex >= 1 && colIndex <= 100) {
                        // 设置属性值
                        String propValue = ensureLength(getStringValue(pr, key), eventAttrLengthLimit);
                        row.setCustomProperty(colIndex, propValue);
                        
                        // 设置属性类型 (使用 $zg_eptp#)
                        String propTypeKey = "$zg_eptp#" + key;
                        String propType = ensureLength(getStringValue(pr, propTypeKey), 256);
                        row.setPropertyType(colIndex, propType);
                    }
                }
            }
        }
    }
    
    // ============ 工具方法 ============
    
    private Set<String> getAttrSet(String dt) {
        if ("mkt".equals(dt)) return mktAttrs;
        if ("abp".equals(dt)) return abpAttrs;
        return Collections.emptySet();
    }
    
    private String getEventNameFromMap(Map<String, Object> pr) {
        String zgEid = getStringValue(pr, "$zg_eid");
        if ("-1".equals(zgEid)) return "st";
        if ("-2".equals(zgEid)) return "se";
        return getStringValue(pr, "$eid");
    }
    
    private String timestampToDateString(Long ct, Integer tz) {
        if (ct == null || tz == null) return NULL_VALUE;
        if (Math.abs(tz) > 48 * 3600 * 1000) return NULL_VALUE;
        try { return DATE_FORMAT.get().format(new Date(ct)); } 
        catch (Exception e) { return NULL_VALUE; }
    }
    
    private Map<String, String> getTimeComponents(Long ct) {
        Map<String, String> result = new HashMap<>();
        try {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(ct);
            result.put("day", DAY_FORMAT.get().format(cal.getTime()));
            result.put("hour", String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)));
            result.put("mill", String.format("%03d", cal.get(Calendar.MILLISECOND)));
        } catch (Exception e) {
            LOG.warn("Failed to get time components", e);
        }
        return result;
    }
    
    private String getYearWeek(String realtime) {
        try {
            Date date = DATE_FORMAT.get().parse(realtime);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DAY_OF_MONTH, -1);
            return YEAR_WEEK_FORMAT.get().format(cal.getTime());
        } catch (Exception e) { return NULL_VALUE; }
    }
    
    private boolean isExpiredTime(String realtime) {
        try {
            Date eventDate = DATE_FORMAT.get().parse(realtime);
            long eventTime = eventDate.getTime();
            
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DAY_OF_MONTH, expireAddDays);
            cal.set(Calendar.HOUR_OF_DAY, 23);
            cal.set(Calendar.MINUTE, 59);
            cal.set(Calendar.SECOND, 59);
            long maxTime = cal.getTimeInMillis();
            
            cal.setTime(new Date());
            cal.add(Calendar.DAY_OF_MONTH, -expireSubDays);
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            long minTime = cal.getTimeInMillis();
            
            return eventTime < minTime || eventTime > maxTime;
        } catch (Exception e) { return true; }
    }
    
    private boolean isSearchEngine(String website) {
        if (isNullOrEmpty(website)) return false;
        return website.contains(WWW_BAIDU_COM) || website.contains(WWW_SOGOU_COM) ||
               website.contains(CN_BING_COM) || website.contains(WWW_SO_COM) ||
               website.contains(M_SM_CN) || website.contains(WWW_GOOGLE_COM) ||
               website.contains(WWW_GOOGLE_CO);
    }
    
    private String getUtmTermFromRef(String utmTerm, String referrerUrl) {
        if (!isNullOrEmpty(utmTerm)) return utmTerm;
        if (isNullOrEmpty(referrerUrl)) return NULL_VALUE;
        
        // 解析 referrer URL 获取关键词 (与Scala ZGMethod.getKeyWords 一致)
        try {
            java.net.URI uri = new java.net.URI(referrerUrl);
            String host = uri.getHost();
            String query = uri.getQuery();
            
            if (query == null) return NULL_VALUE;
            
            Map<String, String> params = parseQueryString(query);
            
            // 根据不同搜索引擎获取关键词参数
            if (host != null) {
                if (host.contains(WWW_SOGOU_COM)) {
                    String keyword = params.get("query");
                    if (keyword != null) return java.net.URLDecoder.decode(keyword, "UTF-8");
                } else if (host.contains(CN_BING_COM) || host.contains(WWW_SO_COM) 
                        || host.contains(M_SM_CN) || host.contains(WWW_GOOGLE_COM) 
                        || host.contains(WWW_GOOGLE_CO)) {
                    String keyword = params.get("q");
                    if (keyword != null) return java.net.URLDecoder.decode(keyword, "UTF-8");
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to parse referrer URL: {}", referrerUrl, e);
        }
        return NULL_VALUE;
    }
    
    private Map<String, String> parseQueryString(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null) return params;
        for (String param : query.split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length == 2) {
                params.put(pair[0], pair[1]);
            }
        }
        return params;
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
    
    private String ensureNetwork(String value) {
        if ("-1".equals(value)) return NULL_VALUE;
        return ensureIntLength(value, 256);
    }
    
    private String ensureIntLength(String value, int maxLength) {
        if (isNullOrEmpty(value) || "null".equals(value)) return NULL_VALUE;
        if (value.length() > 6 || !value.matches("[0-9]*")) return NULL_VALUE;
        return value;
    }
    
    private String ensureIntRange(String value, int min, int max) {
        if (isNullOrEmpty(value)) return NULL_VALUE;
        try {
            int intValue = Integer.parseInt(value);
            if (intValue >= min && intValue <= max) return value;
        } catch (NumberFormatException e) { }
        return NULL_VALUE;
    }
    
    private Long ipToLong(String ip) {
        if (isNullOrEmpty(ip)) return null;
        try {
            String[] parts = ip.split("\\.");
            if (parts.length != 4) return null;
            long result = 0;
            for (int i = 0; i < 4; i++) {
                result = (result << 8) | Integer.parseInt(parts[i]);
            }
            return result;
        } catch (Exception e) { return null; }
    }
}
