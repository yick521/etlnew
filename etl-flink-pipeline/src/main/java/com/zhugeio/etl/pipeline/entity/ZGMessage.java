package com.zhugeio.etl.pipeline.entity;

import com.alibaba.fastjson2.JSONObject;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author ningjh
 * @name com.zhugeio.etl.pipeline.example.ZGMessage
 * @date 2025/12/1
 * @description 消息体，从kafka中获取
 */
public class ZGMessage implements Serializable {

    // 必需字段
    private String topic;
    private Integer partition;
    private Long offset;
    private String key;
    private String rawData;
    
    private Integer result = 0;
    private Integer appId = 0;
    private String appKey = "";
    private Integer sdk = 0;
    private Map<String, Object> data;
    private Map<String, Object> errData;
    private String json = "";
    private String error = "";
    private Integer errorCode = 0;
    private String errorDescribe = "";
    private String business = "";

    // 数据质量需要的输出，侧输出分流使用
    private JSONObject dataQuality;
    private JSONObject dataQualityError;
    // 广告输出kafka信息
    private List<String> advKafkaMsgList;

    public ZGMessage() {
    }

    public ZGMessage(String topic, Integer partition, Long offset, String key, String rawData, Integer result, Integer appId, String appKey, Integer sdk, Map<String, Object> data, Map<String, Object> errData, String json, String error, Integer errorCode, String errorDescribe, String business) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.rawData = rawData;
        this.result = result;
        this.appId = appId;
        this.appKey = appKey;
        this.sdk = sdk;
        this.data = data;
        this.errData = errData;
        this.json = json;
        this.error = error;
        this.errorCode = errorCode;
        this.errorDescribe = errorDescribe;
        this.business = business;
    }

    public List<String> getAdvKafkaMsgList() {
        return advKafkaMsgList;
    }

    public void setAdvKafkaMsgList(List<String> advKafkaMsgList) {
        this.advKafkaMsgList = advKafkaMsgList;
    }

    public JSONObject getDataQualityError() {
        return dataQualityError;
    }

    public void setDataQualityError(JSONObject dataQualityError) {
        this.dataQualityError = dataQualityError;
    }

    public JSONObject getDataQuality() {
        return dataQuality;
    }

    public void setDataQuality(JSONObject dataQuality) {
        this.dataQuality = dataQuality;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getRawData() {
        return rawData;
    }

    public void setRawData(String rawData) {
        this.rawData = rawData;
    }

    public Integer getResult() {
        return result;
    }

    public void setResult(Integer result) {
        this.result = result;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public String getAppKey() {
        return appKey;
    }

    public void setAppKey(String appKey) {
        this.appKey = appKey;
    }

    public Integer getSdk() {
        return sdk;
    }

    public void setSdk(Integer sdk) {
        this.sdk = sdk;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getErrData() {
        return errData;
    }

    public void setErrData(Map<String, Object> errData) {
        this.errData = errData;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public Integer getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(Integer errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorDescribe() {
        return errorDescribe;
    }

    public void setErrorDescribe(String errorDescribe) {
        this.errorDescribe = errorDescribe;
    }

    public String getBusiness() {
        return business;
    }

    public void setBusiness(String business) {
        this.business = business;
    }



    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ZGMessage zgMessage = (ZGMessage) o;
        return Objects.equals(topic, zgMessage.topic) && Objects.equals(partition, zgMessage.partition) && Objects.equals(offset, zgMessage.offset) && Objects.equals(key, zgMessage.key) && Objects.equals(rawData, zgMessage.rawData) && Objects.equals(result, zgMessage.result) && Objects.equals(appId, zgMessage.appId) && Objects.equals(appKey, zgMessage.appKey) && Objects.equals(sdk, zgMessage.sdk) && Objects.equals(data, zgMessage.data) && Objects.equals(errData, zgMessage.errData) && Objects.equals(json, zgMessage.json) && Objects.equals(error, zgMessage.error) && Objects.equals(errorCode, zgMessage.errorCode) && Objects.equals(errorDescribe, zgMessage.errorDescribe) && Objects.equals(business, zgMessage.business);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset, key, rawData, result, appId, appKey, sdk, data, errData, json, error, errorCode, errorDescribe, business);
    }

    @Override
    public String toString() {
        return "com.zhugeio.etl.pipeline.example.ZGMessage{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", key='" + key + '\'' +
                ", rawData='" + rawData + '\'' +
                ", result=" + result +
                ", appId=" + appId +
                ", appKey='" + appKey + '\'' +
                ", sdk=" + sdk +
                ", data=" + data +
                ", errData=" + errData +
                ", json='" + json + '\'' +
                ", error='" + error + '\'' +
                ", errorCode=" + errorCode +
                ", errorDescribe='" + errorDescribe + '\'' +
                ", business='" + business + '\'' +
                '}';
    }
}