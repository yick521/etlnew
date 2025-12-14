package com.zhugeio.etl.pipeline.dataquality;

/**
 * 数据质量错误枚举
 */
public enum ErrorMessageEnum {

    ZG_ZGID_NONE(1030001, "入库核心字段$zg_zgid缺失", "入库核心字段$zg_zgid缺失"),
    ZG_EID_NONE(1030002, "入库核心字段$zg_eid缺失", "入库核心字段$zg_eid缺失"),
    ZG_DID_NONE(1030003, "入库核心字段$zg_did缺失", "入库核心字段$zg_did缺失"),
    CT_TZ_NONE(1030004, "入库核心字段$ct或$tz缺失", "入库核心字段$ct或$tz缺失"),
    EVENT_TIME_EXCEEDS_RANGE(1030005, "事件时间不在配置时间区间内", "dw模块默认配置只入库事件事件在近7天的事件，早于该时间的数据不再入库");

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