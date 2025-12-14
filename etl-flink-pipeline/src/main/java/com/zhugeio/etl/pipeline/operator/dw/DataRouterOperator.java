package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.model.output.*;
import com.zhugeio.etl.pipeline.config.Config;
import com.zhugeio.etl.pipeline.dataquality.DataQualityContext;
import com.zhugeio.etl.pipeline.dataquality.DataQualityKafkaService;
import com.zhugeio.etl.pipeline.dataquality.DataValidator;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.service.BaiduKeywordService;
import com.zhugeio.etl.pipeline.service.EventAttrColumnService;
import com.zhugeio.etl.pipeline.transfer.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据路由算子
 *
 * 对应 Scala: MsgTransfer.transfer
 *
 * 输入: 已富化的 ZGMessage (包含 IP/UA 解析结果)
 * 输出: 根据 dt 类型路由到不同的侧输出
 *
 * 路由规则:
 * - zgid -> UserRow
 * - pl -> DeviceRow
 * - usr -> UserPropertyRow (可能多条)
 * - evt/vtl/mkt/ss/se/abp -> EventAttrRow
 *
 */
public class DataRouterOperator extends ProcessFunction<ZGMessage, EventAttrRow> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DataRouterOperator.class);

    // 侧输出标签
    public static final OutputTag<UserRow> USER_OUTPUT =
            new OutputTag<UserRow>("user-output") {};
    public static final OutputTag<DeviceRow> DEVICE_OUTPUT =
            new OutputTag<DeviceRow>("device-output") {};
    public static final OutputTag<UserPropertyRow> USER_PROPERTY_OUTPUT =
            new OutputTag<UserPropertyRow>("user-property-output") {};

    // 黑名单应用
    private Set<String> blackAppIds;

    // 白名单应用 (用于百度关键词)
    private Set<String> whiteAppIds;

    // CDP 应用
    private Set<Integer> cdpAppIds;

    // 转换器
    private transient UserTransfer userTransfer;
    private transient DeviceTransfer deviceTransfer;
    private transient UserPropertyTransfer userPropertyTransfer;
    private transient EventAttrTransfer eventAttrTransfer;

    // 服务
    private transient EventAttrColumnService columnService;
    private transient BaiduKeywordService keywordService;

    // 数据质量
    private transient DataValidator dataValidator;
    private boolean dataQualityEnabled;


    // 统计
    private final AtomicLong userCount = new AtomicLong(0);
    private final AtomicLong deviceCount = new AtomicLong(0);
    private final AtomicLong userPropertyCount = new AtomicLong(0);
    private final AtomicLong eventAttrCount = new AtomicLong(0);
    private final AtomicLong unknownCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong blacklistedCount = new AtomicLong(0);
    private final AtomicLong dataQualityErrorCount = new AtomicLong(0);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 加载黑名单
        String blackAppsStr = Config.getString(Config.BLACK_APPIDS, "-1");
        blackAppIds = new HashSet<>(Arrays.asList(blackAppsStr.split(",")));

        // 加载白名单 (用于百度关键词)
        String whiteAppsStr = Config.getString(Config.WHITE_APPID, "");
        whiteAppIds = new HashSet<>();
        if (!whiteAppsStr.isEmpty()) {
            whiteAppIds.addAll(Arrays.asList(whiteAppsStr.split(",")));
        }

        // CDP 应用 (TODO: 从配置或数据库加载)
        cdpAppIds = new HashSet<>();

        // 初始化列映射服务
        columnService = new EventAttrColumnService(
                Config.getString(Config.KVROCKS_HOST, "localhost"),
                Config.getInt(Config.KVROCKS_PORT, 6379),
                Config.getBoolean(Config.KVROCKS_CLUSTER, false),
                Config.getInt(Config.KVROCKS_LOCAL_CACHE_SIZE, 10000),
                Config.getInt(Config.KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES, 60)
        );
        columnService.init();

        // 初始化百度关键词服务
        keywordService = new BaiduKeywordService(
                Config.getString(Config.BAIDU_URL, "http://referer.bj.baidubce.com/v1/eqid"),
                Config.getString(Config.BAIDU_ID, ""),
                Config.getString(Config.BAIDU_KEY, ""),
                Config.getString(Config.REDIS_HOST, "localhost"),
                Config.getInt(Config.REDIS_PORT, 6379),
                Config.getBoolean(Config.REDIS_CLUSTER, false),
                Config.getInt(Config.REQUEST_SOCKET_TIMEOUT, 5),
                Config.getInt(Config.REQUEST_CONNECT_TIMEOUT, 5)
        );
        keywordService.init();

        // 初始化转换器
        userTransfer = new UserTransfer();
        deviceTransfer = new DeviceTransfer();

        userPropertyTransfer = new UserPropertyTransfer();
        userPropertyTransfer.setCdpAppIds(cdpAppIds);

        int expireSubDays = Config.getInt(Config.TIME_EXPIRE_SUBDAYS, 7);
        int expireAddDays = Config.getInt(Config.TIME_EXPIRE_ADDDAYS, 1);
        int eventAttrLengthLimit = Config.getInt(Config.EVENT_ATTR_LENGTH_LIMIT, 256);
        eventAttrTransfer = new EventAttrTransfer(columnService, expireSubDays, expireAddDays, eventAttrLengthLimit);

        // 初始化数据质量校验器
        dataQualityEnabled = Config.getBoolean(Config.DQ_ENABLED, true);
        if (dataQualityEnabled) {
            DataQualityKafkaService dqService = DataQualityKafkaService.getInstance();
            dataValidator = new DataValidator(expireSubDays, expireAddDays, dqService);
            LOG.info("DataValidator initialized with subDays={}, addDays={}", expireSubDays, expireAddDays);
        }

        LOG.info("DataRouterOperator initialized: blackAppIds={}, whiteAppIds={}, dataQualityEnabled={}",
                blackAppIds.size(), whiteAppIds.size(), dataQualityEnabled);
    }

    @Override
    public void processElement(ZGMessage message, Context ctx, Collector<EventAttrRow> out) throws Exception {
        try {
            // 直接从 ZGMessage 获取数据
            Map<String, Object> msgData = message.getData();
            if (msgData == null) {
                errorCount.incrementAndGet();
                return;
            }

            // 从 ZGMessage 获取基础信息
            Integer appId = message.getAppId();
            String business = message.getBusiness();

            if (appId == null || appId == 0) {
                errorCount.incrementAndGet();
                return;
            }

            // 黑名单检查
            if (blackAppIds.contains(String.valueOf(appId))) {
                blacklistedCount.incrementAndGet();
                return;
            }

            // 从 data map 获取其他字段
            Integer platform = getIntValue(msgData, "plat", 0);
            String ua = getStringValue(msgData, "ua");
            String ip = getStringValue(msgData, "ip");
            String sdk = getStringValue(msgData, "sdk");
            String pl = getStringValue(msgData, "pl");

            // 获取消息级别的 st 时间戳 (用于 DeviceTransfer)
            Long msgSt = getLongValue(msgData, "st", null);

            // 获取设备 MD5 (来自 usr.did)
            String deviceMd5 = null;
            Object usrObj = msgData.get("usr");
            if (usrObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> usr = (Map<String, Object>) usrObj;
                deviceMd5 = getStringValue(usr, "did");
            }

            // 获取富化数据 (由上游算子填充) - 已修复: 直接从顶层读取
            String[] ipResult = extractIpResultFromMap(msgData);
            Map<String, String> uaResult = extractUaResultFromMap(msgData);
            if (business == null || business.isEmpty()) {
                business = getStringValue(msgData, "business", "\\N");
            }

            // 获取 data 数组
            Object dataObj = msgData.get("data");
            if (dataObj == null) {
                return;
            }

            List<?> dataList = (List<?>) dataObj;
            if (dataList.isEmpty()) {
                return;
            }

            // 预先提取所有 eqid 用于批量查询关键词
            Map<String, String> eqidKeywords = preloadKeywordsFromList(appId, dataList);

            // 原始 JSON (用于错误日志)
            String rawJson = message.getRawData();

            // 处理每个数据项
            for (Object item : dataList) {
                if (item == null) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> dataItem = (Map<String, Object>) item;

                String dt = getStringValue(dataItem, "dt");
                @SuppressWarnings("unchecked")
                Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");

                if (dt == null || pr == null) {
                    continue;
                }

                // 根据 dt 类型路由
                routeByDtFromMap(ctx, out, dt, appId, platform, pr, ip, ipResult, ua, uaResult,
                        business, eqidKeywords, msgSt, deviceMd5, sdk, pl, rawJson);
            }

        } catch (Exception e) {
            errorCount.incrementAndGet();
            LOG.error("Error processing message", e);
        }
    }

    // ============ Map 工具方法 ============

    private String getStringValue(Map<String, Object> map, String key) {
        return getStringValue(map, key, null);
    }

    private String getStringValue(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        return String.valueOf(value);
    }

    private Integer getIntValue(Map<String, Object> map, String key, Integer defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private Long getLongValue(Map<String, Object> map, String key, Long defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 根据 dt 类型路由数据 (使用 Map)
     */
    private void routeByDtFromMap(Context ctx, Collector<EventAttrRow> out, String dt,
                                  Integer appId, Integer platform, Map<String, Object> pr,
                                  String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                  String business, Map<String, String> eqidKeywords,
                                  Long msgSt, String deviceMd5, String sdk, String pl, String rawJson) {

        switch (dt) {
            case "zgid":
                // 用户映射表
                UserRow userRow = userTransfer.transferFromMap(appId, platform, pr);
                if (userRow != null) {
                    ctx.output(USER_OUTPUT, userRow);
                    userCount.incrementAndGet();
                }
                break;

            case "pl":
                // 设备表
                DeviceRow deviceRow = deviceTransfer.transferFromMap(appId, platform, pr, msgSt, deviceMd5);
                if (deviceRow != null) {
                    ctx.output(DEVICE_OUTPUT, deviceRow);
                    deviceCount.incrementAndGet();
                }
                break;

            case "usr":
                // 用户属性表 (可能产生多条)
                List<UserPropertyRow> propRows = userPropertyTransfer.transferFromMap(appId, platform, pr);
                for (UserPropertyRow propRow : propRows) {
                    ctx.output(USER_PROPERTY_OUTPUT, propRow);
                    userPropertyCount.incrementAndGet();
                }
                break;

            case "evt":
            case "vtl":
            case "mkt":
            case "ss":
            case "se":
            case "abp":
                // 事件属性表 - 需要数据质量校验
                processEventAttr(out, dt, appId, platform, pr, ip, ipResult, ua, uaResult,
                        business, eqidKeywords, sdk, pl, rawJson);
                break;

            default:
                unknownCount.incrementAndGet();
                break;
        }
    }

    /**
     * 处理事件属性 (使用 DataValidator 校验)
     *
     * 对应 Scala: EventAttrTransfer.transfer 中的校验逻辑
     */
    private void processEventAttr(Collector<EventAttrRow> out, String dt,
                                  Integer appId, Integer platform, Map<String, Object> pr,
                                  String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                  String business, Map<String, String> eqidKeywords,
                                  String sdk, String pl, String rawJson) {

        // 提取校验所需字段
        String zgZgid = getStringValue(pr, "$zg_zgid");
        String zgEid = getStringValue(pr, "$zg_eid");
        String zgDid = getStringValue(pr, "$zg_did");
        Long ct = getLongValue(pr, "$ct", null);
        String tz = getStringValue(pr, "$tz");
        String eventName = getStringValue(pr, "$eid", "\\N");

        // 计算格式化时间
        String realTime = formatTime(ct, tz);

        // ============ 使用 DataValidator 校验 ============
        if (dataQualityEnabled && dataValidator != null) {
            // 构建校验上下文
            DataQualityContext dqCtx = DataQualityContext.builder()
                    .appId(appId)
                    .platform(platform)
                    .eventName(eventName)
                    .eventId(zgEid)
                    .zgZgid(zgZgid)
                    .zgDid(zgDid)
                    .ct(ct)
                    .tz(tz)
                    .sdk(sdk)
                    .pl(pl)
                    .rawJson(rawJson)
                    .build();

            // 执行校验
            DataValidator.ValidationResult result = dataValidator.validate(dqCtx, realTime);
            if (result.isFailed()) {
                dataQualityErrorCount.incrementAndGet();
                return; // 校验失败，丢弃记录
            }
        }

        // ============ 校验通过，转换并输出 ============
        EventAttrRow eventRow = eventAttrTransfer.transferFromMap(
                appId, platform, dt, pr, ip, ipResult, ua, uaResult, business, eqidKeywords);
        if (eventRow != null) {
            out.collect(eventRow);
            eventAttrCount.incrementAndGet();
        }
    }

    /**
     * 格式化时间戳为日期时间字符串
     *
     * 对应 Scala: ZGMethod.TimeStamp2Date
     */
    private String formatTime(Long ct, String tz) {
        if (ct == null || tz == null || "\\N".equals(tz)) {
            return "\\N";
        }

        try {
            long tzOffset = Long.parseLong(tz);

            // 时区范围检查 (±48小时)
            if (tzOffset > 48 * 3600 * 1000 || tzOffset < -48 * 3600 * 1000) {
                return "\\N";
            }

            java.util.Calendar cal = java.util.Calendar.getInstance();
            cal.setTimeInMillis(ct);

            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String result = sdf.format(cal.getTime());

            if (result.length() > 19) {
                return "\\N";
            }

            return result;
        } catch (Exception e) {
            return "\\N";
        }
    }

    /**
     * 预加载百度关键词 (使用 List)
     */
    private Map<String, String> preloadKeywordsFromList(Integer appId, List<?> dataList) {
        Map<String, String> result = new HashMap<>();

        // 只对白名单应用启用
        if (!whiteAppIds.contains(String.valueOf(appId))) {
            return result;
        }

        Set<String> eqids = new HashSet<>();

        for (Object item : dataList) {
            if (item == null) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> dataItem = (Map<String, Object>) item;

            String dt = getStringValue(dataItem, "dt");
            if (dt == null || !keywordService.shouldExtractKeyword(dt)) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");
            if (pr == null) {
                continue;
            }

            // 检查是否已有 utm_term
            String utmTerm = getStringValue(pr, "$utm_term");
            if (utmTerm != null && !utmTerm.isEmpty() && !"\\N".equals(utmTerm)) {
                continue;
            }

            // 提取 eqid
            String ref = getStringValue(pr, "$ref");
            String eqid = keywordService.extractEqid(ref);
            if (eqid != null) {
                eqids.add(eqid);
                // 回写 eqid 到 pr
                pr.put("$eqid", eqid);
            }
        }

        // 批量查询
        if (!eqids.isEmpty()) {
            try {
                result = keywordService.preloadKeywordsAsync(eqids).get();
            } catch (Exception e) {
                LOG.warn("Failed to preload keywords", e);
            }
        }

        return result;
    }

    /**
     * 从 Map 提取 IP 解析结果 (已修复)
     *
     * 修复说明: IpEnrichOperator 直接写入顶层字段 (country/province/city)
     * 而不是 _enriched.ip 结构，因此需要直接从顶层读取
     *
     * 对应 Scala: MsgTransfer.prepareContext 中的 context.ipResult
     */
    private String[] extractIpResultFromMap(Map<String, Object> msgData) {
        // 直接从顶层字段读取 (与 IpEnrichOperator 写入位置一致)
        return new String[]{
                getStringValue(msgData, "country", "\\N"),
                getStringValue(msgData, "province", "\\N"),
                getStringValue(msgData, "city", "\\N")
        };
    }


    /**
     * 从 Map 提取 UA 解析结果 (已修复)
     *
     * 修复说明: UserAgentEnrichOperator 直接写入顶层字段 (os/browser 等)
     * 而不是 _enriched.ua 结构，因此需要直接从顶层读取
     *
     * 对应 Scala: MsgTransfer.prepareContext 中的 context.ur (UserAgent 解析结果)
     */
    private Map<String, String> extractUaResultFromMap(Map<String, Object> msgData) {
        Map<String, String> result = new HashMap<>();

        // 直接从顶层字段读取 (与 UserAgentEnrichOperator 写入位置一致)
        result.put("os", getStringValue(msgData, "os", null));
        result.put("os_version", getStringValue(msgData, "os_version", null));
        result.put("browser", getStringValue(msgData, "browser", null));
        result.put("browser_version", getStringValue(msgData, "browser_version", null));

        return result;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing DataRouterOperator - Stats: user={}, device={}, userProperty={}, " +
                        "eventAttr={}, unknown={}, error={}, blacklisted={}",
                userCount.get(), deviceCount.get(), userPropertyCount.get(),
                eventAttrCount.get(), unknownCount.get(), errorCount.get(), blacklistedCount.get());

        if (columnService != null) {
            columnService.close();
        }

        if (keywordService != null) {
            keywordService.close();
        }

        super.close();
    }

    // Getters for stats
    public long getUserCount() { return userCount.get(); }
    public long getDeviceCount() { return deviceCount.get(); }
    public long getUserPropertyCount() { return userPropertyCount.get(); }
    public long getEventAttrCount() { return eventAttrCount.get(); }
    public long getUnknownCount() { return unknownCount.get(); }
    public long getErrorCount() { return errorCount.get(); }
}
