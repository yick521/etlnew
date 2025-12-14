package com.zhugeio.etl.pipeline.dataquality;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 时间校验器
 *
 * 对应 Scala: MsgTransfer.isExpireTime
 */
public class TimeValidator implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMAT_BEGIN =
            DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00.000");
    private static final DateTimeFormatter TIME_FORMAT_END =
            DateTimeFormatter.ofPattern("yyyy-MM-dd 23:59:59.999");

    private static volatile ConcurrentHashMap<String, Long> timeCache = new ConcurrentHashMap<>();

    private final int subDays;
    private final int addDays;

    public TimeValidator(int subDays, int addDays) {
        this.subDays = subDays;
        this.addDays = addDays;
    }

    /**
     * 检查事件时间是否过期
     */
    public boolean isExpireTime(String timestampStr, String sdk) {
        // zg_server 和 zg-cdp 不检查时间
        if ("zg_server".equals(sdk) || "zg-cdp".equals(sdk)) {
            return false;
        }

        if (isNullOrEmpty(timestampStr)) {
            return true;
        }

        try {
            long ctTime = Timestamp.valueOf(timestampStr).getTime();
            long[] timeRange = getTimeRange();
            return (ctTime < timeRange[0]) || (ctTime > timeRange[1]);
        } catch (Exception e) {
            return true;
        }
    }

    private long[] getTimeRange() {
        LocalDateTime timePoint = LocalDateTime.now();
        String currentDateStr = TIME_FORMAT.format(timePoint);

        if (timeCache.containsKey(currentDateStr)) {
            return new long[]{
                    timeCache.get("beginTimeInMills"),
                    timeCache.get("endTimeInMills")
            };
        }

        synchronized (TimeValidator.class) {
            if (timeCache.containsKey(currentDateStr)) {
                return new long[]{
                        timeCache.get("beginTimeInMills"),
                        timeCache.get("endTimeInMills")
                };
            }

            timeCache = new ConcurrentHashMap<>();

            LocalDateTime endPoint = timePoint.plus(addDays, ChronoUnit.DAYS);
            String endStr = TIME_FORMAT_END.format(endPoint);
            long endTimeInMills = Timestamp.valueOf(endStr).getTime();

            LocalDateTime beginPoint = timePoint.minus(subDays, ChronoUnit.DAYS);
            String beginStr = TIME_FORMAT_BEGIN.format(beginPoint);
            long beginTimeInMills = Timestamp.valueOf(beginStr).getTime();

            timeCache.put(currentDateStr, 0L);
            timeCache.put("beginTimeInMills", beginTimeInMills);
            timeCache.put("endTimeInMills", endTimeInMills);

            return new long[]{beginTimeInMills, endTimeInMills};
        }
    }

    public static boolean isNullOrEmpty(String str) {
        return str == null || str.trim().isEmpty() || "\\N".equals(str);
    }
}