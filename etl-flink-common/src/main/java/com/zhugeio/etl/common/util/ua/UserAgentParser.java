package com.zhugeio.etl.common.util.ua;

import com.zhugeio.etl.common.model.ua.UserAgentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User-Agent解析器
 *
 * 基于正则表达式的轻量级UA解析器
 * 参考原 Spark 代码中的 useragentanalysis.UserAgent
 */
public class UserAgentParser implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(UserAgentParser.class);
    private static final long serialVersionUID = 1L;

    // ========== 操作系统正则 ==========
    private static final Pattern ANDROID_PATTERN = Pattern.compile(
            "Android[\\s/]([0-9.]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern IOS_PATTERN = Pattern.compile(
            "(iPhone|iPad|iPod).*OS\\s+([0-9_]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern WINDOWS_PATTERN = Pattern.compile(
            "Windows NT ([0-9.]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern MAC_PATTERN = Pattern.compile(
            "Mac OS X ([0-9_]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern LINUX_PATTERN = Pattern.compile(
            "Linux", Pattern.CASE_INSENSITIVE);

    // ========== 浏览器正则 ==========
    private static final Pattern CHROME_PATTERN = Pattern.compile(
            "Chrome/([0-9.]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern SAFARI_PATTERN = Pattern.compile(
            "Version/([0-9.]+).*Safari", Pattern.CASE_INSENSITIVE);

    private static final Pattern FIREFOX_PATTERN = Pattern.compile(
            "Firefox/([0-9.]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern EDGE_PATTERN = Pattern.compile(
            "Edg/([0-9.]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern IE_PATTERN = Pattern.compile(
            "MSIE ([0-9.]+)|Trident/.*rv:([0-9.]+)", Pattern.CASE_INSENSITIVE);

    // ========== 设备品牌正则 ==========
    private static final Pattern HUAWEI_PATTERN = Pattern.compile(
            "(Huawei|HUAWEI|Honor|HONOR)\\s*([A-Z0-9\\s-]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern XIAOMI_PATTERN = Pattern.compile(
            "(MI|Xiaomi|Redmi)\\s*([A-Z0-9\\s-]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern OPPO_PATTERN = Pattern.compile(
            "(OPPO|Realme)\\s*([A-Z0-9\\s-]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern VIVO_PATTERN = Pattern.compile(
            "(vivo|VIVO)\\s*([A-Z0-9\\s-]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern SAMSUNG_PATTERN = Pattern.compile(
            "SAMSUNG\\s*([A-Z0-9\\s-]+)|SM-([A-Z0-9]+)", Pattern.CASE_INSENSITIVE);

    private static final Pattern ONEPLUS_PATTERN = Pattern.compile(
            "OnePlus\\s*([A-Z0-9\\s-]+)", Pattern.CASE_INSENSITIVE);

    /**
     * 解析User-Agent字符串
     */
    public UserAgentInfo parse(String ua) {
        UserAgentInfo info = new UserAgentInfo(ua);

        if (ua == null || ua.trim().isEmpty()) {
            setDefaults(info);
            return info;
        }

        try {
            // 解析操作系统
            parseOperatingSystem(ua, info);

            // 解析浏览器
            parseBrowser(ua, info);

            // 解析设备
            parseDevice(ua, info);

        } catch (Exception e) {
            LOG.error("UA解析异常: {}", ua, e);
            setDefaults(info);
        }

        return info;
    }

    /**
     * 解析操作系统
     */
    private void parseOperatingSystem(String ua, UserAgentInfo info) {
        // Android
        Matcher androidMatcher = ANDROID_PATTERN.matcher(ua);
        if (androidMatcher.find()) {
            info.setOsName("Android");
            info.setOsFamily("Android");
            info.setOsVersion(androidMatcher.group(1));
            return;
        }

        // iOS
        Matcher iosMatcher = IOS_PATTERN.matcher(ua);
        if (iosMatcher.find()) {
            String device = iosMatcher.group(1);
            String version = iosMatcher.group(2).replace("_", ".");

            info.setOsName("iOS");
            info.setOsFamily("iOS");
            info.setOsVersion(version);

            // 设置设备类型
            if ("iPad".equalsIgnoreCase(device)) {
                info.setDeviceType("Tablet");
                info.setDeviceBrand("Apple");
                info.setDeviceModel("iPad");
            } else if ("iPhone".equalsIgnoreCase(device)) {
                info.setDeviceType("Mobile");
                info.setDeviceBrand("Apple");
                info.setDeviceModel("iPhone");
            } else if ("iPod".equalsIgnoreCase(device)) {
                info.setDeviceType("Mobile");
                info.setDeviceBrand("Apple");
                info.setDeviceModel("iPod");
            }
            return;
        }

        // Windows
        Matcher windowsMatcher = WINDOWS_PATTERN.matcher(ua);
        if (windowsMatcher.find()) {
            info.setOsName("Windows");
            info.setOsFamily("Windows");
            info.setOsVersion(mapWindowsVersion(windowsMatcher.group(1)));
            info.setDeviceType("Desktop");
            return;
        }

        // Mac OS X
        Matcher macMatcher = MAC_PATTERN.matcher(ua);
        if (macMatcher.find()) {
            info.setOsName("Mac OS X");
            info.setOsFamily("Mac OS");
            info.setOsVersion(macMatcher.group(1).replace("_", "."));
            info.setDeviceType("Desktop");
            info.setDeviceBrand("Apple");
            return;
        }

        // Linux
        if (LINUX_PATTERN.matcher(ua).find()) {
            info.setOsName("Linux");
            info.setOsFamily("Linux");
            info.setDeviceType("Desktop");
            return;
        }

        // 默认
        info.setOsName("Unknown");
        info.setOsFamily("Unknown");
    }

    /**
     * 解析浏览器
     */
    private void parseBrowser(String ua, UserAgentInfo info) {
        // Edge (必须在Chrome之前检测,因为Edge也包含Chrome)
        Matcher edgeMatcher = EDGE_PATTERN.matcher(ua);
        if (edgeMatcher.find()) {
            info.setBrowserName("Edge");
            info.setBrowserFamily("Edge");
            info.setBrowserVersion(edgeMatcher.group(1));
            return;
        }

        // Chrome
        Matcher chromeMatcher = CHROME_PATTERN.matcher(ua);
        if (chromeMatcher.find()) {
            info.setBrowserName("Chrome");
            info.setBrowserFamily("Chrome");
            info.setBrowserVersion(chromeMatcher.group(1));
            return;
        }

        // Safari (必须在Chrome之后检测)
        Matcher safariMatcher = SAFARI_PATTERN.matcher(ua);
        if (safariMatcher.find()) {
            info.setBrowserName("Safari");
            info.setBrowserFamily("Safari");
            info.setBrowserVersion(safariMatcher.group(1));
            return;
        }

        // Firefox
        Matcher firefoxMatcher = FIREFOX_PATTERN.matcher(ua);
        if (firefoxMatcher.find()) {
            info.setBrowserName("Firefox");
            info.setBrowserFamily("Firefox");
            info.setBrowserVersion(firefoxMatcher.group(1));
            return;
        }

        // Internet Explorer
        Matcher ieMatcher = IE_PATTERN.matcher(ua);
        if (ieMatcher.find()) {
            info.setBrowserName("Internet Explorer");
            info.setBrowserFamily("IE");
            String version = ieMatcher.group(1) != null ? ieMatcher.group(1) : ieMatcher.group(2);
            info.setBrowserVersion(version);
            return;
        }

        // 默认
        info.setBrowserName("Unknown");
        info.setBrowserFamily("Unknown");
    }

    /**
     * 解析设备信息
     */
    private void parseDevice(String ua, UserAgentInfo info) {
        // 如果已经从iOS检测中设置了设备,跳过
        if (info.getDeviceBrand() != null && "Apple".equals(info.getDeviceBrand())) {
            return;
        }

        // Huawei / Honor
        Matcher huaweiMatcher = HUAWEI_PATTERN.matcher(ua);
        if (huaweiMatcher.find()) {
            String brand = huaweiMatcher.group(1);
            String model = huaweiMatcher.group(2);
            info.setDeviceBrand(capitalize(brand));
            info.setDeviceModel(model != null ? model.trim() : "");
            info.setDeviceType("Mobile");
            return;
        }

        // Xiaomi / Redmi
        Matcher xiaomiMatcher = XIAOMI_PATTERN.matcher(ua);
        if (xiaomiMatcher.find()) {
            String brand = xiaomiMatcher.group(1);
            String model = xiaomiMatcher.group(2);
            info.setDeviceBrand(capitalize(brand));
            info.setDeviceModel(model != null ? model.trim() : "");
            info.setDeviceType("Mobile");
            return;
        }

        // OPPO / Realme
        Matcher oppoMatcher = OPPO_PATTERN.matcher(ua);
        if (oppoMatcher.find()) {
            String brand = oppoMatcher.group(1);
            String model = oppoMatcher.group(2);
            info.setDeviceBrand(capitalize(brand));
            info.setDeviceModel(model != null ? model.trim() : "");
            info.setDeviceType("Mobile");
            return;
        }

        // Vivo
        Matcher vivoMatcher = VIVO_PATTERN.matcher(ua);
        if (vivoMatcher.find()) {
            String brand = vivoMatcher.group(1);
            String model = vivoMatcher.group(2);
            info.setDeviceBrand("Vivo");
            info.setDeviceModel(model != null ? model.trim() : "");
            info.setDeviceType("Mobile");
            return;
        }

        // Samsung
        Matcher samsungMatcher = SAMSUNG_PATTERN.matcher(ua);
        if (samsungMatcher.find()) {
            String model = samsungMatcher.group(1) != null ?
                    samsungMatcher.group(1) : samsungMatcher.group(2);
            info.setDeviceBrand("Samsung");
            info.setDeviceModel(model != null ? model.trim() : "");
            info.setDeviceType("Mobile");
            return;
        }

        // OnePlus
        Matcher oneplusMatcher = ONEPLUS_PATTERN.matcher(ua);
        if (oneplusMatcher.find()) {
            String model = oneplusMatcher.group(1);
            info.setDeviceBrand("OnePlus");
            info.setDeviceModel(model != null ? model.trim() : "");
            info.setDeviceType("Mobile");
            return;
        }

        // 根据OS推断设备类型
        if (info.getDeviceType() == null) {
            if ("Android".equals(info.getOsName())) {
                info.setDeviceType("Mobile");
            } else if ("iOS".equals(info.getOsName())) {
                info.setDeviceType("Mobile");
            } else if ("Windows".equals(info.getOsName()) ||
                    "Mac OS X".equals(info.getOsName()) ||
                    "Linux".equals(info.getOsName())) {
                info.setDeviceType("Desktop");
            }
        }
    }

    /**
     * 映射Windows版本号
     */
    private String mapWindowsVersion(String ntVersion) {
        switch (ntVersion) {
            case "10.0": return "10";
            case "6.3": return "8.1";
            case "6.2": return "8";
            case "6.1": return "7";
            case "6.0": return "Vista";
            case "5.1": return "XP";
            default: return ntVersion;
        }
    }

    /**
     * 首字母大写
     */
    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }

    /**
     * 设置默认值
     */
    private void setDefaults(UserAgentInfo info) {
        if (info.getOsName() == null) {
            info.setOsName("Unknown");
        }
        if (info.getOsFamily() == null) {
            info.setOsFamily("Unknown");
        }
        if (info.getBrowserName() == null) {
            info.setBrowserName("Unknown");
        }
        if (info.getBrowserFamily() == null) {
            info.setBrowserFamily("Unknown");
        }
        if (info.getDeviceType() == null) {
            info.setDeviceType("Unknown");
        }
    }
}