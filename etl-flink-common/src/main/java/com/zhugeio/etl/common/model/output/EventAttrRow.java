package com.zhugeio.etl.common.model.output;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * 事件属性表行数据 (b_user_event_attr_{appId})
 * 
 * Doris 表结构 (242列):
 * - 列 0-39: 基础字段 (40列)
 * - 列 40-139: 自定义属性值 cus1-cus100 (100列)
 * - 列 140-239: 属性类型 type1-type100 (100列)
 * - 列 240-241: 分区字段 eid, yw (2列)
 * 
 * Doris 列顺序 (与 Kudu 不同):
 * 0:zg_id, 1:session_id, 2:uuid, 3:event_id, 
 * 4:begin_day_id, 5:begin_date, 6:begin_time_id,  <-- Doris: day在date前面
 * 7:device_id, 8:user_id, 9:event_name, ...
 */
public class EventAttrRow implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    // 总列数
    public static final int TOTAL_COLUMNS = 242;
    
    // 基础字段数
    public static final int BASIC_COLUMNS = 40;
    
    // 自定义属性数
    public static final int CUSTOM_COLUMNS = 100;
    
    // 类型列数
    public static final int TYPE_COLUMNS = 100;
    
    // 分区字段数
    public static final int PARTITION_COLUMNS = 2;
    
    // 自定义属性起始索引
    public static final int CUS_START_INDEX = 40;
    
    // 类型列起始索引
    public static final int TYPE_START_INDEX = 140;
    
    // 分区字段索引
    public static final int EID_INDEX = 240;
    public static final int YW_INDEX = 241;
    
    // NULL 值表示
    public static final String NULL_VALUE = "\\N";
    
    // 应用ID
    private Integer appId;
    
    // 事件ID (用于分区)
    private String eventId;
    
    // 242列数据
    private final List<String> columns;
    
    public EventAttrRow() {
        this.columns = new ArrayList<>(TOTAL_COLUMNS);
        // 初始化所有列为 NULL
        for (int i = 0; i < TOTAL_COLUMNS; i++) {
            columns.add(NULL_VALUE);
        }
    }
    
    public EventAttrRow(Integer appId) {
        this();
        this.appId = appId;
    }
    
    // ============ 基础字段设置方法 (Doris 列顺序) ============
    
    /** 列0: 诸葛ID */
    public void setZgId(String value) { setColumn(0, value); }
    
    /** 列1: 会话ID */
    public void setSessionId(String value) { setColumn(1, value); }
    
    /** 列2: UUID (区分同一时刻触发的同一事件) */
    public void setUuid(String value) { setColumn(2, value); }
    
    /** 列3: 事件ID (zg_eid) */
    public void setEventIdColumn(String value) { 
        setColumn(3, value); 
        this.eventId = value;
    }
    
    /** 列4: 日期ID (yyyyMMdd) - Doris 在 date 前面 */
    public void setBeginDayId(String value) { setColumn(4, value); }
    
    /** 列5: 开始时间戳 (秒) */
    public void setBeginDate(String value) { setColumn(5, value); }
    
    /** 列6: 时间ID (HH0SSS) */
    public void setBeginTimeId(String value) { setColumn(6, value); }
    
    /** 列7: 设备ID */
    public void setDeviceId(String value) { setColumn(7, value); }
    
    /** 列8: 用户ID */
    public void setUserId(String value) { setColumn(8, value); }
    
    /** 列9: 事件名称 ($eid) */
    public void setEventName(String value) { setColumn(9, value); }
    
    /** 列10: 平台 */
    public void setPlatform(Integer value) { setColumn(10, value != null ? String.valueOf(value) : NULL_VALUE); }
    
    /** 列11: 网络类型 */
    public void setNetwork(String value) { setColumn(11, value); }
    
    /** 列12: 运营商 (mccmnc) */
    public void setMccmnc(String value) { setColumn(12, value); }
    
    /** 列13: UserAgent */
    public void setUseragent(String value) { setColumn(13, value); }
    
    /** 列14: 来源网站 */
    public void setWebsite(String value) { setColumn(14, value); }
    
    /** 列15: 当前URL */
    public void setCurrentUrl(String value) { setColumn(15, value); }
    
    /** 列16: 来源URL */
    public void setReferrerUrl(String value) { setColumn(16, value); }
    
    /** 列17: 渠道 */
    public void setChannel(String value) { setColumn(17, value); }
    
    /** 列18: 应用版本 */
    public void setAppVersion(String value) { setColumn(18, value); }
    
    /** 列19: IP (数字格式) */
    public void setIp(Long value) { setColumn(19, value != null ? String.valueOf(value) : NULL_VALUE); }
    
    /** 列20: IP (字符串格式) */
    public void setIpStr(String value) { setColumn(20, value); }
    
    /** 列21: 国家 */
    public void setCountry(String value) { setColumn(21, value); }
    
    /** 列22: 地区/省份 */
    public void setArea(String value) { setColumn(22, value); }
    
    /** 列23: 城市 */
    public void setCity(String value) { setColumn(23, value); }
    
    /** 列24: 操作系统 */
    public void setOs(String value) { setColumn(24, value); }
    
    /** 列25: 操作系统版本 */
    public void setOv(String value) { setColumn(25, value); }
    
    /** 列26: 浏览器 */
    public void setBs(String value) { setColumn(26, value); }
    
    /** 列27: 浏览器版本 */
    public void setBv(String value) { setColumn(27, value); }
    
    /** 列28: 广告来源 */
    public void setUtmSource(String value) { setColumn(28, value); }
    
    /** 列29: 广告媒介 */
    public void setUtmMedium(String value) { setColumn(29, value); }
    
    /** 列30: 广告名称 */
    public void setUtmCampaign(String value) { setColumn(30, value); }
    
    /** 列31: 广告内容 */
    public void setUtmContent(String value) { setColumn(31, value); }
    
    /** 列32: 广告关键词 */
    public void setUtmTerm(String value) { setColumn(32, value); }
    
    /** 列33: 持续时间 (毫秒) */
    public void setDuration(String value) { setColumn(33, value); }
    
    /** 列34: UTC时间戳 */
    public void setUtcDate(String value) { setColumn(34, value); }
    
    /** 列35: attr1 (业务标识) */
    public void setAttr1(String value) { setColumn(35, value); }
    
    /** 列36: attr2 (微信小程序appId) */
    public void setAttr2(String value) { setColumn(36, value); }
    
    /** 列37: attr3 */
    public void setAttr3(String value) { setColumn(37, value); }
    
    /** 列38: attr4 */
    public void setAttr4(String value) { setColumn(38, value); }
    
    /** 列39: attr5 (zgid_sessionid) */
    public void setAttr5(String value) { setColumn(39, value); }
    
    // ============ 自定义属性设置方法 ============
    
    /**
     * 设置自定义属性值
     * @param colIndex 列索引 1-100
     * @param value 属性值
     */
    public void setCustomProperty(int colIndex, String value) {
        if (colIndex >= 1 && colIndex <= CUSTOM_COLUMNS) {
            setColumn(CUS_START_INDEX - 1 + colIndex, value);
        }
    }
    
    /**
     * 设置属性类型
     * @param colIndex 列索引 1-100
     * @param type 属性类型
     */
    public void setPropertyType(int colIndex, String type) {
        if (colIndex >= 1 && colIndex <= TYPE_COLUMNS) {
            setColumn(TYPE_START_INDEX - 1 + colIndex, type);
        }
    }
    
    // ============ 分区字段设置方法 ============
    
    /** 列240: 事件ID分区字段 */
    public void setEid(String value) { setColumn(EID_INDEX, value); }
    
    /** 列241: 年周 */
    public void setYw(String value) { setColumn(YW_INDEX, value); }
    
    // ============ 通用方法 ============
    
    private void setColumn(int index, String value) {
        if (index >= 0 && index < TOTAL_COLUMNS) {
            columns.set(index, value != null && !value.isEmpty() ? value : NULL_VALUE);
        }
    }
    
    public String getColumn(int index) {
        if (index >= 0 && index < TOTAL_COLUMNS) {
            return columns.get(index);
        }
        return NULL_VALUE;
    }
    
    public List<String> getColumns() {
        return columns;
    }
    
    public Integer getAppId() {
        return appId;
    }
    
    public void setAppId(Integer appId) {
        this.appId = appId;
    }
    
    public String getEventId() {
        return eventId;
    }
    
    /**
     * 获取表名
     */
    public String getTableName() {
        return "b_user_event_attr_" + appId;
    }
    
    /**
     * 获取去重键
     * 格式: zg_id + session_id + uuid + event_id + begin_day_id + begin_date
     */
    public String getDedupeKey() {
        return columns.get(0) + "_" + columns.get(1) + "_" + columns.get(2) + "_" + 
               columns.get(3) + "_" + columns.get(4) + "_" + columns.get(5);
    }
    
    /**
     * 转换为 TSV 格式 (Tab分隔)
     */
    public String toTsv() {
        StringJoiner joiner = new StringJoiner("\t");
        for (String col : columns) {
            joiner.add(col);
        }
        return joiner.toString();
    }
    
    @Override
    public String toString() {
        return "EventAttrRow{" +
                "appId=" + appId +
                ", eventId='" + eventId + '\'' +
                ", zgId='" + columns.get(0) + '\'' +
                ", eventName='" + columns.get(9) + '\'' +
                '}';
    }
}
