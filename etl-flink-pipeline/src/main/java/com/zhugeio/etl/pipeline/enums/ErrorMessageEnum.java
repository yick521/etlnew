package com.zhugeio.etl.pipeline.enums;

import java.io.Serializable;

public enum ErrorMessageEnum implements Serializable {

    IP_BLOCK(1010001, "ip为黑名单", "Ip被配置为黑名单，详见mysql表data_access_filter"),
    UA_BLOCK(1010002, "ua为黑名单", "user-agent被配置为黑名单，详见mysql表data_access_filter"),
    IP_NONE(1010003, "Ip字段获取异常", "Ip字段不存在或不是字符串类型"),
    NOW_NONE(1010004, "Now字段获取异常", "Now字段不存在或不是数值类型"),

    BASIC_SCHEMA_FORMAT_NOT_MATCH(1020002, "转换后的json与basicSchema标准不符", "json中的要字段不存在或json结构有误，详见basicSchema文档"),

    HEADER_NONE(1010005, "Header字段解析异常", "Header字段不存在或不是字符串类型"),
    
    AK_NONE(1010006, "ak在应用管理中不存在或已被删除", "ak在应用管理中不存在或已被删除");

    private final int errorCode;
    private final String errorMessage;
    private final String errorRuleDescribe;

    ErrorMessageEnum(int errorCode, String errorMessage, String errorRuleDescribe) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.errorRuleDescribe = errorRuleDescribe;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getErrorRuleDescribe() {
        return errorRuleDescribe;
    }

}