package com.zhugeio.etl.pipeline.dataquality;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 数据校验器
 *
 * 对应 Scala: EventAttrTransfer 中的校验逻辑
 *
 * 在 Operator 阶段使用，校验失败发送错误日志
 */
public class DataValidator implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DataValidator.class);

    private final TimeValidator timeValidator;
    private final DataQualityKafkaService kafkaService;

    public DataValidator(int subDays, int addDays, DataQualityKafkaService kafkaService) {
        this.timeValidator = new TimeValidator(subDays, addDays);
        this.kafkaService = kafkaService;
    }

    /**
     * 校验事件数据
     *
     * @param ctx 数据质量上下文
     * @param realTime 格式化后的时间 (yyyy-MM-dd HH:mm:ss)
     * @return 校验结果
     */
    public ValidationResult validate(DataQualityContext ctx, String realTime) {

        // 1. 校验 $zg_zgid
        if (TimeValidator.isNullOrEmpty(ctx.getZgZgid())) {
            LOG.warn("=======> 入库核心字段$zg_zgid缺失, appId={}", ctx.getAppId());
            sendErrorLog(ErrorMessageEnum.ZG_ZGID_NONE, ctx);
            return ValidationResult.fail(ErrorMessageEnum.ZG_ZGID_NONE);
        }

        // 2. 校验 $zg_eid
        if (TimeValidator.isNullOrEmpty(ctx.getEventId())) {
            LOG.warn("=======> 入库核心字段$zg_eid缺失, appId={}", ctx.getAppId());
            sendErrorLog(ErrorMessageEnum.ZG_EID_NONE, ctx);
            return ValidationResult.fail(ErrorMessageEnum.ZG_EID_NONE);
        }

        // 3. 校验 $zg_did
        if (TimeValidator.isNullOrEmpty(ctx.getZgDid())) {
            LOG.warn("=======> 入库核心字段$zg_did缺失, appId={}", ctx.getAppId());
            sendErrorLog(ErrorMessageEnum.ZG_DID_NONE, ctx);
            return ValidationResult.fail(ErrorMessageEnum.ZG_DID_NONE);
        }

        // 4. 校验 $ct 和 $tz
        if (realTime == null || "\\N".equals(realTime) ||
                TimeValidator.isNullOrEmpty(ctx.getTz())) {
            LOG.warn("=======> 入库核心字段$ct或$tz缺失, appId={}", ctx.getAppId());
            sendErrorLog(ErrorMessageEnum.CT_TZ_NONE, ctx);
            return ValidationResult.fail(ErrorMessageEnum.CT_TZ_NONE);
        }

        // 5. 校验事件时间范围
        if (timeValidator.isExpireTime(realTime, ctx.getSdk())) {
            LOG.warn("=======> 事件时间不在配置时间区间内, appId={}, time={}",
                    ctx.getAppId(), realTime);
            sendErrorLog(ErrorMessageEnum.EVENT_TIME_EXCEEDS_RANGE, ctx);
            return ValidationResult.fail(ErrorMessageEnum.EVENT_TIME_EXCEEDS_RANGE);
        }

        return ValidationResult.success();
    }

    private void sendErrorLog(ErrorMessageEnum errorEnum, DataQualityContext ctx) {
        if (kafkaService != null && kafkaService.isEnabled()) {
            kafkaService.sendErrorLog(
                    errorEnum,
                    ctx.getAppId(),
                    ctx.getPlatform(),
                    ctx.getEventName(),
                    ctx.getRawJson(),
                    ctx.getCt(),
                    ctx.getPl(),
                    ctx.getSdk()
            );
        }
    }

    /**
     * 刷新错误计数 (Checkpoint 时调用)
     */
    public void flushErrorCounts() {
        if (kafkaService != null && kafkaService.isEnabled()) {
            kafkaService.flushCounts();
        }
    }

    /**
     * 校验结果
     */
    public static class ValidationResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean success;
        private final ErrorMessageEnum error;

        private ValidationResult(boolean success, ErrorMessageEnum error) {
            this.success = success;
            this.error = error;
        }

        public static ValidationResult success() {
            return new ValidationResult(true, null);
        }

        public static ValidationResult fail(ErrorMessageEnum error) {
            return new ValidationResult(false, error);
        }

        public boolean isSuccess() { return success; }
        public boolean isFailed() { return !success; }
        public ErrorMessageEnum getError() { return error; }
    }
}
