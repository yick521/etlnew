package com.zhugeio.etl.pipeline.service;

import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.HexUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SmUtil;
import cn.hutool.crypto.asymmetric.KeyType;
import cn.hutool.crypto.asymmetric.SM2;
import cn.hutool.crypto.symmetric.SymmetricCrypto;

import com.zhugeio.etl.pipeline.config.Config;
import com.zhugeio.etl.pipeline.exceptions.ResolveException;
import com.zhugeio.etl.pipeline.util.SecretUtils;
import com.zhugeio.tool.commons.JsonUtil;
import com.zhugeio.tool.commons.ZlibUtil;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;


/**
 * @author ningjh
 * @name MsgResolver
 * @date 2025/11/28
 * @description 消息解析
 */
public class MsgResolver implements Serializable {

//    public static String privateKey = Config.getProp(Config.PRIVATE_KEY);
    public static String privateKey = "";

    public static String resolve(String data) throws Exception {
        //json解析异常
        Map<String, Object> total = JsonUtil.mapFromJson(data);
//        System.out.println("total = "+total);

        // Ip获取异常
        String ip = null;
        Object ipObj = total.get("Ip");
        if (!(ipObj instanceof String)) {
            throw new ResolveException("ip not right:" + data);
        } else {
            ip =  (String) ipObj;
        }

        // Now获取异常
        Long sts = null;
        Object now = total.get("Now");
        if (!(now instanceof Number)) {
            throw new ResolveException("Now not right:" + data);
        } else {
            sts =  new BigDecimal(String.valueOf(now)).longValue();
        }

        // Header解析异常
        Map<String, Object> header = null;
        Object headerObj = total.get("Header");
        if (!(headerObj instanceof String)) {
            throw new ResolveException("Header not right:" + data);
        }
        header =  JsonUtil.mapFromJson((String) headerObj);

        // user-agent获取异常
        String userAgent = null;
        Object userAgentObj = header.get("user-agent");
        if (userAgentObj instanceof String) {
            userAgent = (String) userAgentObj;
        }else {
            userAgent = "DEFAULT";
        }

        //Args解析异常
        Object argsObj = total.get("Args");
        if (!(argsObj instanceof String)) {
            throw new ResolveException("[Args解析异常] Args not right:" + data);
        }
        Map<String, Object> args = JsonUtil.mapFromJson((String) argsObj);
        if (args.isEmpty()) {
            throw new ResolveException("[Args解析异常] Args not right:" + data);
        }

        String content = null;
        content = getContent(args);

        Map<String, Object> result = JsonUtil.mapFromJson(content);

        //event解析异常 event值不是json格式
        if (result == null) {
            throw new ResolveException("[event解析异常] data is :" + data + ",content is :" + content);
        }

        result.put("ip", ip);
        result.put("st", sts);
        result.put("ua", userAgent);

        return JsonUtil.toJson(result);
    }

    public static String getContent(Map<String, Object> args) throws Exception {
        // gate模块:encrypt获取异常
        boolean isEncryped = isEncryped(args);
        // gate模块:compress获取异常
        boolean isCompressed = isCompressed(args);
        if (isEncryped) {
            String type = String.valueOf(args.get("type"));
            String key = String.valueOf(args.get("key"));
            String event = String.valueOf(args.get("event"));
            if ("2".equals(type)) {
                String sm2PriKeySecret = Config.getProp(Config.SM2_PRIKEY);
                SM2 sm2 = SmUtil.sm2(sm2PriKeySecret, null);
                if (!key.startsWith("04")) {
                    key = "04" + key;
                }
                String decryptData = StrUtil.utf8Str(sm2.decryptFromBcd(key, KeyType.PrivateKey));
                String[] strings = decryptData.split(",");
                String decrypt = "";
                if (strings.length > 0) {
                    decrypt = strings[0];
                } else {
                    decrypt = decryptData;
                }
                byte[] sm4key = HexUtil.decodeHex(decrypt);
                SymmetricCrypto sm4 = SmUtil.sm4(sm4key);
                return sm4.decryptStr(event, CharsetUtil.CHARSET_UTF_8);
            } else {
                //读出已经加密的私钥
                String priKeySecret = Config.readResource(Config.getProp(Config.PRIVATE_KEY_PATH));
                // gate模块:event解密异常
                //在进行解密私钥
                String priKey = SecretUtils.decryptionPass(priKeySecret);
                String keyPlain = DecodeService.rsaDecrypt(key, priKey);
                String[] keys = keyPlain.split(",");
                return DecodeService.aesDecode(event, keys[0], keys[1]);
            }
        } else {
            if (!isCompressed) {
                return (String) args.get("event");
            } else {
                String decode = ZlibUtil.decodeAll(((String) args.get("event")).replaceAll(" ", "+"));
                // gate模块:event解压缩异常
                if (decode == null) {
                    throw new ResolveException("[event解压缩异常] event decode not right:" + args);
                }
                return decode;
            }
        }

    }


    public static boolean isCompressed(Map<String, Object> args) {
        Object compress = args.get("compress");
        if (compress == null) {
            return false;
        } else if (!(compress instanceof String)) {
            throw new ResolveException("[compress获取异常] args->compress not right:" + args);
        }

        if ("0".equals(compress)) {
            return false;
        } else if ("1".equals(compress)) {
            return true;
        } else {
            throw new ResolveException("[compress获取异常] args->compress not right:" + args);
        }

    }

    public static boolean isEncryped(Map<String, Object> args) {
        Object encrypt = args.get("encrypt");
        if (encrypt == null) {
            return false;
        } else if (encrypt != null && (!(encrypt instanceof String))) {
            throw new ResolveException("[encrypt获取异常] args->encrypt not right:" + args);
        }

        if ("0".equals(encrypt)) {
            return false;
        } else if ("1".equals(encrypt)) {
            return true;
        } else {
            throw new ResolveException("[encrypt获取异常] args->encrypt not right:" + args);
        }

    }
}
