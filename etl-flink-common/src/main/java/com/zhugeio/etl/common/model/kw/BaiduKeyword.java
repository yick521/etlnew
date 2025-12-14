package com.zhugeio.etl.common.model.kw;

import java.io.Serializable;

/**
 * 百度关键字解析结果
 */
public class BaiduKeyword implements Serializable {
    private static final long serialVersionUID = 1L;

    private String keyword;           // 解析出的关键字
    private String searchEngine;      // 搜索引擎: baidu, google, sogou, bing等
    private String originalUrl;       // 原始URL
    private boolean parsed;           // 是否成功解析

    public BaiduKeyword() {
    }

    public BaiduKeyword(String keyword, String searchEngine, String originalUrl) {
        this.keyword = keyword;
        this.searchEngine = searchEngine;
        this.originalUrl = originalUrl;
        this.parsed = keyword != null && !keyword.isEmpty();
    }

    // Getters and Setters
    public String getKeyword() { return keyword; }
    public void setKeyword(String keyword) { this.keyword = keyword; }

    public String getSearchEngine() { return searchEngine; }
    public void setSearchEngine(String searchEngine) { this.searchEngine = searchEngine; }

    public String getOriginalUrl() { return originalUrl; }
    public void setOriginalUrl(String originalUrl) { this.originalUrl = originalUrl; }

    public boolean isParsed() { return parsed; }
    public void setParsed(boolean parsed) { this.parsed = parsed; }

    @Override
    public String toString() {
        return String.format("BaiduKeyword{engine='%s', keyword='%s', parsed=%s}",
                searchEngine, keyword, parsed);
    }
}
