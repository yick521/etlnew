package com.zhugeio.etl.common.util.kw;

import com.zhugeio.etl.common.model.kw.BaiduKeyword;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 搜索引擎关键字解析器
 *
 * 支持的搜索引擎:
 * - 百度 (baidu.com)
 * - Google (google.com)
 * - 搜狗 (sogou.com)
 * - 360搜索 (so.com)
 * - 必应 (bing.com)
 * - 神马搜索 (sm.cn)
 * - 头条搜索 (toutiao.com)
 */
public class SearchKeywordParser implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SearchKeywordParser.class);
    private static final long serialVersionUID = 1L;

    // 搜索引擎域名匹配规则
    private static final Map<Pattern, String> ENGINE_PATTERNS = new HashMap<>();

    // 关键字参数名映射 (搜索引擎 -> 参数名)
    private static final Map<String, String[]> KEYWORD_PARAMS = new HashMap<>();

    static {
        // 初始化搜索引擎识别规则
        ENGINE_PATTERNS.put(Pattern.compile("baidu\\.com", Pattern.CASE_INSENSITIVE), "baidu");
        ENGINE_PATTERNS.put(Pattern.compile("google\\.(com|cn|com\\.hk)", Pattern.CASE_INSENSITIVE), "google");
        ENGINE_PATTERNS.put(Pattern.compile("sogou\\.com", Pattern.CASE_INSENSITIVE), "sogou");
        ENGINE_PATTERNS.put(Pattern.compile("so\\.com", Pattern.CASE_INSENSITIVE), "360");
        ENGINE_PATTERNS.put(Pattern.compile("bing\\.com", Pattern.CASE_INSENSITIVE), "bing");
        ENGINE_PATTERNS.put(Pattern.compile("sm\\.cn", Pattern.CASE_INSENSITIVE), "shenma");
        ENGINE_PATTERNS.put(Pattern.compile("toutiao\\.com", Pattern.CASE_INSENSITIVE), "toutiao");
        ENGINE_PATTERNS.put(Pattern.compile("yahoo\\.com", Pattern.CASE_INSENSITIVE), "yahoo");

        // 初始化关键字参数映射
        KEYWORD_PARAMS.put("baidu", new String[]{"wd", "word", "kw"});
        KEYWORD_PARAMS.put("google", new String[]{"q", "query"});
        KEYWORD_PARAMS.put("sogou", new String[]{"query", "keyword"});
        KEYWORD_PARAMS.put("360", new String[]{"q"});
        KEYWORD_PARAMS.put("bing", new String[]{"q"});
        KEYWORD_PARAMS.put("shenma", new String[]{"q"});
        KEYWORD_PARAMS.put("toutiao", new String[]{"keyword"});
        KEYWORD_PARAMS.put("yahoo", new String[]{"p"});
    }

    /**
     * 解析URL中的搜索关键字
     */
    public BaiduKeyword parse(String url) {
        if (url == null || url.trim().isEmpty()) {
            return new BaiduKeyword("", "unknown", url);
        }

        try {
            // 1. 识别搜索引擎
            String engine = identifySearchEngine(url);
            if ("unknown".equals(engine)) {
                return new BaiduKeyword("", "unknown", url);
            }

            // 2. 提取关键字参数
            String keyword = extractKeyword(url, engine);

            // 3. URL解码
            if (keyword != null && !keyword.isEmpty()) {
                keyword = UrlDecoder.decodeMultiCharset(keyword);
                // 清理关键字
                keyword = cleanKeyword(keyword);
            }

            return new BaiduKeyword(keyword, engine, url);

        } catch (Exception e) {
            LOG.error("关键字解析失败: {}", url, e);
            return new BaiduKeyword("", "unknown", url);
        }
    }

    /**
     * 识别搜索引擎
     */
    private String identifySearchEngine(String url) {
        for (Map.Entry<Pattern, String> entry : ENGINE_PATTERNS.entrySet()) {
            Matcher matcher = entry.getKey().matcher(url);
            if (matcher.find()) {
                return entry.getValue();
            }
        }
        return "unknown";
    }

    /**
     * 提取关键字参数
     */
    private String extractKeyword(String url, String engine) {
        String[] paramNames = KEYWORD_PARAMS.get(engine);
        if (paramNames == null) {
            return "";
        }

        // 提取URL参数部分
        int queryStart = url.indexOf('?');
        if (queryStart == -1) {
            return "";
        }

        String queryString = url.substring(queryStart + 1);

        // 分割参数
        String[] params = queryString.split("&");

        // 查找关键字参数
        for (String param : params) {
            int equalIndex = param.indexOf('=');
            if (equalIndex == -1) {
                continue;
            }

            String paramName = param.substring(0, equalIndex);
            String paramValue = param.substring(equalIndex + 1);

            // 检查是否为关键字参数
            for (String keywordParam : paramNames) {
                if (paramName.equalsIgnoreCase(keywordParam)) {
                    return paramValue;
                }
            }
        }

        return "";
    }

    /**
     * 清理关键字
     */
    private String cleanKeyword(String keyword) {
        if (keyword == null || keyword.isEmpty()) {
            return "";
        }

        // 去除首尾空格
        keyword = keyword.trim();

        // 移除可能的引号
        keyword = keyword.replaceAll("^\"|\"$", "");
        keyword = keyword.replaceAll("^'|'$", "");

        // 移除特殊字符 (保留中文、英文、数字、空格)
        // keyword = keyword.replaceAll("[^\\u4e00-\\u9fa5a-zA-Z0-9\\s]", "");

        return keyword;
    }

    /**
     * 判断是否为搜索引擎URL
     */
    public boolean isSearchEngineUrl(String url) {
        if (url == null || url.isEmpty()) {
            return false;
        }

        String engine = identifySearchEngine(url);
        return !"unknown".equals(engine);
    }
}