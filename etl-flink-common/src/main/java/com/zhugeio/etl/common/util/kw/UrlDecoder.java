package com.zhugeio.etl.common.util.kw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * URL解码工具类
 */
public class UrlDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(UrlDecoder.class);

    /**
     * URL解码 (支持多种编码)
     */
    public static String decode(String url) {
        if (url == null || url.isEmpty()) {
            return url;
        }

        try {
            // 尝试UTF-8解码
            String decoded = URLDecoder.decode(url, StandardCharsets.UTF_8.name());

            // 如果还包含%,继续解码 (处理双重编码)
            if (decoded.contains("%")) {
                decoded = URLDecoder.decode(decoded, StandardCharsets.UTF_8.name());
            }

            return decoded;

        } catch (UnsupportedEncodingException e) {
            LOG.error("URL解码失败: {}", url, e);
            return url;
        } catch (IllegalArgumentException e) {
            // 无效的编码格式,返回原始值
            LOG.debug("URL解码参数非法: {}", url);
            return url;
        }
    }

    /**
     * 尝试多种编码解码
     */
    public static String decodeMultiCharset(String url) {
        if (url == null || url.isEmpty()) {
            return url;
        }

        String[] charsets = {"UTF-8", "GBK", "GB2312", "ISO-8859-1"};

        for (String charset : charsets) {
            try {
                String decoded = URLDecoder.decode(url, charset);

                // 简单判断是否为中文
                if (containsChinese(decoded)) {
                    return decoded;
                }

            } catch (Exception e) {
                // 继续尝试下一个编码
            }
        }

        // 都失败,返回原始值
        return url;
    }

    /**
     * 判断是否包含中文
     */
    private static boolean containsChinese(String str) {
        if (str == null) return false;

        for (char c : str.toCharArray()) {
            if (c >= 0x4E00 && c <= 0x9FA5) {
                return true;
            }
        }
        return false;
    }
}
