package com.zhugeio.etl.pipeline.entity;

import com.alibaba.fastjson.JSONObject;

import java.util.Map;

/**
 * @author ningjh
 * @name com.zhugeio.etl.pipeline.example.AdvMessage
 * @date 2025/12/1
 * @description 广告的对象
 */
public class AdvMessage {
    // 应用id
    private long zg_appid = 0L;
    // 链接id
    private long lid = 0L;
    // 广告投放位置
    private long csite = 0L;
    // 操作系统平台
    private int ados = -1;
    // 渠道id
    private long channel_id = 0L;
    // 广告主id 由数值型改成字符串
    private String channel_account_id = "";
    // 转化id
    private long convert_id = 0L;
    // 广告组id
    private long channel_campaign_id = 0L;
    // 创意样式
    private long ctype = 0L;
    // 广告创意ID
    private long channel_ad_id = 0L;
    // 广告点击事件的时间
    private long click_time = 0L;
    // 广告计划id
    private long channel_adgroup_id = 0L;
    // ct
    private long ct = 0L;

    private String lname = "";
    private String utm_campaign = "";
    private String utm_source = "";
    private String utm_medium = "";
    private String utm_term = "";
    private String utm_content = "";
    private String channel_ad_name = "";
    private String channel_campaign_name = "";
    private String ip = "";
    private String ua = "";
    private String mac = "";
    private String mac1 = "";
    private String callback_url = "";
    private String geo = "";
    private String imei = "";
    private String idfa = "";
    private String idfa_md5 = "";
    private String sl = "";
    private String model = "";
    private String union_site = "";
    private String android_id = "";
    private String oaid = "";
    private String oaid_md5 = "";
    private String channel_adgroup_name = "";
    private String requers_id = "";
    private String app_key = "";
    private String muid = "";

    private String callback = "";
    // 渠道类型必须
    private String channel_type = "";
    // 推广类型必须
    private String push_type = "";
    // 百度账号akey(监测链接里获取)
    private String akey = "";
    // 百度账号token(监测链接里获取)
    private String token = "";

    private long channel_keyword_id = 0L;
    private String qaid_caa = "";
    private String ip_ua_key = "";
    private String muid_key = "";

    private String idfa_key = "";
    private String imei_key = "";
    private String android_id_key = "";
    private String oaid_key = "";
    private String channel_click_id_key = "";

    private String company = "";
    private String auth_account = "";
    // idfa_key  imei_key  android_id_key oaid_key channel_click_id_key

    // 华为广告 新增字段
    private String hw_app_id = "";
    // vivo新增字段
    private String creative_id = "";

    private String is_delete = "false";
    private String channel_click_id = "";

    // Getter 和 Setter 方法
    public long getZg_appid() {
        return zg_appid;
    }

    public void setZg_appid(long zg_appid) {
        this.zg_appid = zg_appid;
    }

    public long getLid() {
        return lid;
    }

    public void setLid(long lid) {
        this.lid = lid;
    }

    public long getCsite() {
        return csite;
    }

    public void setCsite(long csite) {
        this.csite = csite;
    }

    public int getAdos() {
        return ados;
    }

    public void setAdos(int ados) {
        this.ados = ados;
    }

    public long getChannel_id() {
        return channel_id;
    }

    public void setChannel_id(long channel_id) {
        this.channel_id = channel_id;
    }

    public String getChannel_account_id() {
        return channel_account_id;
    }

    public void setChannel_account_id(String channel_account_id) {
        this.channel_account_id = channel_account_id;
    }

    public long getConvert_id() {
        return convert_id;
    }

    public void setConvert_id(long convert_id) {
        this.convert_id = convert_id;
    }

    public long getChannel_campaign_id() {
        return channel_campaign_id;
    }

    public void setChannel_campaign_id(long channel_campaign_id) {
        this.channel_campaign_id = channel_campaign_id;
    }

    public long getCtype() {
        return ctype;
    }

    public void setCtype(long ctype) {
        this.ctype = ctype;
    }

    public long getChannel_ad_id() {
        return channel_ad_id;
    }

    public void setChannel_ad_id(long channel_ad_id) {
        this.channel_ad_id = channel_ad_id;
    }

    public long getClick_time() {
        return click_time;
    }

    public void setClick_time(long click_time) {
        this.click_time = click_time;
    }

    public long getChannel_adgroup_id() {
        return channel_adgroup_id;
    }

    public void setChannel_adgroup_id(long channel_adgroup_id) {
        this.channel_adgroup_id = channel_adgroup_id;
    }

    public long getCt() {
        return ct;
    }

    public void setCt(long ct) {
        this.ct = ct;
    }

    public String getLname() {
        return lname;
    }

    public void setLname(String lname) {
        this.lname = lname;
    }

    public String getUtm_campaign() {
        return utm_campaign;
    }

    public void setUtm_campaign(String utm_campaign) {
        this.utm_campaign = utm_campaign;
    }

    public String getUtm_source() {
        return utm_source;
    }

    public void setUtm_source(String utm_source) {
        this.utm_source = utm_source;
    }

    public String getUtm_medium() {
        return utm_medium;
    }

    public void setUtm_medium(String utm_medium) {
        this.utm_medium = utm_medium;
    }

    public String getUtm_term() {
        return utm_term;
    }

    public void setUtm_term(String utm_term) {
        this.utm_term = utm_term;
    }

    public String getUtm_content() {
        return utm_content;
    }

    public void setUtm_content(String utm_content) {
        this.utm_content = utm_content;
    }

    public String getChannel_ad_name() {
        return channel_ad_name;
    }

    public void setChannel_ad_name(String channel_ad_name) {
        this.channel_ad_name = channel_ad_name;
    }

    public String getChannel_campaign_name() {
        return channel_campaign_name;
    }

    public void setChannel_campaign_name(String channel_campaign_name) {
        this.channel_campaign_name = channel_campaign_name;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUa() {
        return ua;
    }

    public void setUa(String ua) {
        this.ua = ua;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getMac1() {
        return mac1;
    }

    public void setMac1(String mac1) {
        this.mac1 = mac1;
    }

    public String getCallback_url() {
        return callback_url;
    }

    public void setCallback_url(String callback_url) {
        this.callback_url = callback_url;
    }

    public String getGeo() {
        return geo;
    }

    public void setGeo(String geo) {
        this.geo = geo;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getIdfa() {
        return idfa;
    }

    public void setIdfa(String idfa) {
        this.idfa = idfa;
    }

    public String getIdfa_md5() {
        return idfa_md5;
    }

    public void setIdfa_md5(String idfa_md5) {
        this.idfa_md5 = idfa_md5;
    }

    public String getSl() {
        return sl;
    }

    public void setSl(String sl) {
        this.sl = sl;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getUnion_site() {
        return union_site;
    }

    public void setUnion_site(String union_site) {
        this.union_site = union_site;
    }

    public String getAndroid_id() {
        return android_id;
    }

    public void setAndroid_id(String android_id) {
        this.android_id = android_id;
    }

    public String getOaid() {
        return oaid;
    }

    public void setOaid(String oaid) {
        this.oaid = oaid;
    }

    public String getOaid_md5() {
        return oaid_md5;
    }

    public void setOaid_md5(String oaid_md5) {
        this.oaid_md5 = oaid_md5;
    }

    public String getChannel_adgroup_name() {
        return channel_adgroup_name;
    }

    public void setChannel_adgroup_name(String channel_adgroup_name) {
        this.channel_adgroup_name = channel_adgroup_name;
    }

    public String getRequers_id() {
        return requers_id;
    }

    public void setRequers_id(String requers_id) {
        this.requers_id = requers_id;
    }

    public String getApp_key() {
        return app_key;
    }

    public void setApp_key(String app_key) {
        this.app_key = app_key;
    }

    public String getMuid() {
        return muid;
    }

    public void setMuid(String muid) {
        this.muid = muid;
    }

    public String getCallback() {
        return callback;
    }

    public void setCallback(String callback) {
        this.callback = callback;
    }

    public String getChannel_type() {
        return channel_type;
    }

    public void setChannel_type(String channel_type) {
        this.channel_type = channel_type;
    }

    public String getPush_type() {
        return push_type;
    }

    public void setPush_type(String push_type) {
        this.push_type = push_type;
    }

    public String getAkey() {
        return akey;
    }

    public void setAkey(String akey) {
        this.akey = akey;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public long getChannel_keyword_id() {
        return channel_keyword_id;
    }

    public void setChannel_keyword_id(long channel_keyword_id) {
        this.channel_keyword_id = channel_keyword_id;
    }

    public String getQaid_caa() {
        return qaid_caa;
    }

    public void setQaid_caa(String qaid_caa) {
        this.qaid_caa = qaid_caa;
    }

    public String getIp_ua_key() {
        return ip_ua_key;
    }

    public void setIp_ua_key(String ip_ua_key) {
        this.ip_ua_key = ip_ua_key;
    }

    public String getMuid_key() {
        return muid_key;
    }

    public void setMuid_key(String muid_key) {
        this.muid_key = muid_key;
    }

    public String getIdfa_key() {
        return idfa_key;
    }

    public void setIdfa_key(String idfa_key) {
        this.idfa_key = idfa_key;
    }

    public String getImei_key() {
        return imei_key;
    }

    public void setImei_key(String imei_key) {
        this.imei_key = imei_key;
    }

    public String getAndroid_id_key() {
        return android_id_key;
    }

    public void setAndroid_id_key(String android_id_key) {
        this.android_id_key = android_id_key;
    }

    public String getOaid_key() {
        return oaid_key;
    }

    public void setOaid_key(String oaid_key) {
        this.oaid_key = oaid_key;
    }

    public String getChannel_click_id_key() {
        return channel_click_id_key;
    }

    public void setChannel_click_id_key(String channel_click_id_key) {
        this.channel_click_id_key = channel_click_id_key;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getAuth_account() {
        return auth_account;
    }

    public void setAuth_account(String auth_account) {
        this.auth_account = auth_account;
    }

    public String getHw_app_id() {
        return hw_app_id;
    }

    public void setHw_app_id(String hw_app_id) {
        this.hw_app_id = hw_app_id;
    }

    public String getCreative_id() {
        return creative_id;
    }

    public void setCreative_id(String creative_id) {
        this.creative_id = creative_id;
    }

    public String getIs_delete() {
        return is_delete;
    }

    public void setIs_delete(String is_delete) {
        this.is_delete = is_delete;
    }

    public String getChannel_click_id() {
        return channel_click_id;
    }

    public void setChannel_click_id(String channel_click_id) {
        this.channel_click_id = channel_click_id;
    }

    /**
     * 转换为JSON字符串
     */
    public String toJsonString() {
        JSONObject json = new JSONObject();
        json.put("zg_appid", zg_appid);
        json.put("lid", lid);
        json.put("csite", csite);
        json.put("ados", ados);
        json.put("channel_id", channel_id);
        json.put("channel_account_id", channel_account_id);
        json.put("convert_id", convert_id);
        json.put("channel_campaign_id", channel_campaign_id);
        json.put("ctype", ctype);
        json.put("channel_ad_id", channel_ad_id);
        json.put("click_time", click_time);
        json.put("channel_adgroup_id", channel_adgroup_id);
        json.put("ct", ct);
        json.put("lname", lname);
        json.put("utm_campaign", utm_campaign);
        json.put("utm_source", utm_source);
        json.put("utm_medium", utm_medium);
        json.put("utm_term", utm_term);
        json.put("utm_content", utm_content);
        json.put("channel_ad_name", channel_ad_name);
        json.put("channel_campaign_name", channel_campaign_name);
        json.put("ip", ip);
        json.put("ua", ua);
        json.put("mac", mac);
        json.put("mac1", mac1);
        json.put("callback_url", callback_url);
        json.put("geo", geo);
        json.put("imei", imei);
        json.put("idfa", idfa);
        json.put("idfa_md5", idfa_md5);
        json.put("sl", sl);
        json.put("model", model);
        json.put("union_site", union_site);
        json.put("android_id", android_id);
        json.put("oaid", oaid);
        json.put("oaid_md5", oaid_md5);
        json.put("channel_adgroup_name", channel_adgroup_name);
        json.put("requers_id", requers_id);
        json.put("app_key", app_key);
        json.put("muid", muid);
        json.put("callback", callback);
        json.put("channel_type", channel_type);
        json.put("push_type", push_type);
        json.put("akey", akey);
        json.put("token", token);
        json.put("channel_keyword_id", channel_keyword_id);
        json.put("qaid_caa", qaid_caa);
        json.put("ip_ua_key", ip_ua_key);
        json.put("muid_key", muid_key);
        json.put("idfa_key", idfa_key);
        json.put("imei_key", imei_key);
        json.put("android_id_key", android_id_key);
        json.put("oaid_key", oaid_key);
        json.put("channel_click_id_key", channel_click_id_key);
        json.put("company", company);
        json.put("auth_account", auth_account);
        json.put("creative_id", creative_id);
        json.put("hw_app_id", hw_app_id);
        json.put("is_delete", is_delete);
        json.put("channel_click_id", channel_click_id);
        
        return json.toJSONString();
    }

    /**
     * 为APP端广告信息赋值（字段名以$开头）
     */
    public void setFields(Map<?, ?> props) {
        this.csite = getLongValue(props.get("$csite"), 0L);
        // app端广告信息中 $ados ： android 0 、 ios 1 、其他 3
        this.ados = getIntValue(props.get("$ados"), 3);
        this.channel_id = getLongValue(props.get("$channel_id"), 0L);
        this.channel_account_id = getStringValue(props.get("$channel_account_id"), "0");
        this.convert_id = getLongValue(props.get("$convert_id"), 0L);
        this.channel_campaign_id = getLongValue(props.get("$channel_campaign_id"), 0L);
        this.ctype = getLongValue(props.get("$ctype"), 0L);
        this.channel_ad_id = getLongValue(props.get("$channel_ad_id"), 0L);

        this.channel_adgroup_id = getLongValue(props.get("$channel_adgroup_id"), 0L);
        this.lname = getStringValue(props.get("$lname"), "");
        this.utm_campaign = getStringValue(props.get("$utm_campaign"), "");
        this.utm_source = getStringValue(props.get("$utm_source"), "");
        this.utm_medium = getStringValue(props.get("$utm_medium"), "");
        this.utm_term = getStringValue(props.get("$utm_term"), "");
        this.utm_content = getStringValue(props.get("$utm_content"), "");
        this.channel_ad_name = getStringValue(props.get("$channel_ad_name"), "");
        this.channel_campaign_name = getStringValue(props.get("$channel_campaign_name"), "");
        this.ip = getStringValue(props.get("$ip"), "");
        this.ua = getStringValue(props.get("$ua"), "");
        this.mac = getStringValue(props.get("$mac"), "");
        this.mac1 = getStringValue(props.get("$mac1"), "");
        this.callback_url = getStringValue(props.get("$callback_url"), "");
        this.geo = getStringValue(props.get("$geo"), "");
        this.imei = getStringValue(props.get("$imei"), "");
        this.idfa = getStringValue(props.get("$idfa"), "");
        this.idfa_md5 = getStringValue(props.get("$idfa_md5"), "");
        this.sl = getStringValue(props.get("$sl"), "");
        this.model = getStringValue(props.get("$model"), "");
        this.union_site = getStringValue(props.get("$union_site"), "");
        this.android_id = getStringValue(props.get("$android_id"), "");
        this.oaid = getStringValue(props.get("$oaid"), "");
        this.oaid_md5 = getStringValue(props.get("$oaid_md5"), "");
        this.channel_adgroup_name = getStringValue(props.get("$channel_adgroup_name"), "");
        this.requers_id = getStringValue(props.get("$requers_id"), "");
        this.muid = getStringValue(props.get("$muid"), "");
        this.callback = getStringValue(props.get("$callback"), "");
        this.channel_type = getStringValue(props.get("$channel_type"), "");
        this.push_type = getStringValue(props.get("$push_type"), "");
        this.channel_keyword_id = getLongValue(props.get("$channel_keyword_id"), 0L);
        this.qaid_caa = getStringValue(props.get("$qaid_caa"), "");
        this.akey = getStringValue(props.get("$akey"), "");
        this.token = getStringValue(props.get("$token"), "");
        this.company = getStringValue(props.get("$company"), "");
        this.auth_account = getStringValue(props.get("$auth_account"), "");
        this.creative_id = getStringValue(props.get("$creative_id"), "");
        this.channel_click_id = getStringValue(props.get("$channel_click_id"), "");
        this.hw_app_id = getStringValue(props.get("$hw_app_id"), "");
    }

    /**
     * 为WEB端广告信息赋值（字段名不以$开头）
     */
    public void setFieldsWithout(Map<String, Object> props) {
        this.csite = getLongValue(props.get("csite"), 0L);
        // app端广告信息中 ados ： android 0 、 ios 1 、其他 3
        this.ados = getIntValue(props.get("ados"), 3);
        this.channel_id = getLongValue(props.get("channel_id"), 0L);
        this.channel_account_id = getStringValue(props.get("channel_account_id"), "0");
        this.convert_id = getLongValue(props.get("convert_id"), 0L);
        this.channel_campaign_id = getLongValue(props.get("channel_campaign_id"), 0L);
        this.ctype = getLongValue(props.get("ctype"), 0L);
        this.channel_ad_id = getLongValue(props.get("channel_ad_id"), 0L);

        this.channel_adgroup_id = getLongValue(props.get("channel_adgroup_id"), 0L);
        this.lname = getStringValue(props.get("lname"), "");
        this.utm_campaign = getStringValue(props.get("utm_campaign"), "");
        this.utm_source = getStringValue(props.get("utm_source"), "");
        this.utm_medium = getStringValue(props.get("utm_medium"), "");
        this.utm_term = getStringValue(props.get("utm_term"), "");
        this.utm_content = getStringValue(props.get("utm_content"), "");
        this.channel_ad_name = getStringValue(props.get("channel_ad_name"), "");
        this.channel_campaign_name = getStringValue(props.get("channel_campaign_name"), "");
        this.ip = getStringValue(props.get("ip"), "");
        this.ua = getStringValue(props.get("ua"), "");
        this.mac = getStringValue(props.get("mac"), "");
        this.mac1 = getStringValue(props.get("mac1"), "");
        this.callback_url = getStringValue(props.get("callback_url"), "");
        this.geo = getStringValue(props.get("geo"), "");
        this.imei = getStringValue(props.get("imei"), "");
        this.idfa = getStringValue(props.get("idfa"), "");
        this.idfa_md5 = getStringValue(props.get("idfa_md5"), "");
        this.sl = getStringValue(props.get("sl"), "");
        this.model = getStringValue(props.get("model"), "");
        this.union_site = getStringValue(props.get("union_site"), "");
        this.android_id = getStringValue(props.get("android_id"), "");
        this.oaid = getStringValue(props.get("oaid"), "");
        this.oaid_md5 = getStringValue(props.get("oaid_md5"), "");
        this.channel_adgroup_name = getStringValue(props.get("channel_adgroup_name"), "");
        this.requers_id = getStringValue(props.get("requers_id"), "");
        this.muid = getStringValue(props.get("muid"), "");
        this.callback = getStringValue(props.get("callback"), "");
        this.channel_type = getStringValue(props.get("channel_type"), "");
        this.push_type = getStringValue(props.get("push_type"), "");
        this.channel_keyword_id = getLongValue(props.get("channel_keyword_id"), 0L);
        this.qaid_caa = getStringValue(props.get("qaid_caa"), "");
        this.akey = getStringValue(props.get("akey"), "");
        this.token = getStringValue(props.get("token"), "");
        this.company = getStringValue(props.get("company"), "");
        this.auth_account = getStringValue(props.get("auth_account"), "");
        this.creative_id = getStringValue(props.get("creative_id"), "");
        this.channel_click_id = getStringValue(props.get("channel_click_id"), "");
        this.hw_app_id = getStringValue(props.get("hw_app_id"), "");
    }

    // 辅助方法：安全地获取Long值
    private long getLongValue(Object value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            } else {
                return Long.parseLong(String.valueOf(value));
            }
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    // 辅助方法：安全地获取Int值
    private int getIntValue(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            if (value instanceof Number) {
                return ((Number) value).intValue();
            } else {
                return Integer.parseInt(String.valueOf(value));
            }
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    // 辅助方法：安全地获取String值
    private String getStringValue(Object value, String defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return String.valueOf(value);
    }

    @Override
    public String toString() {
        return "AdMessage{" +
                "zg_appid=" + zg_appid +
                ", lid=" + lid +
                ", csite=" + csite +
                ", ados=" + ados +
                ", channel_id=" + channel_id +
                ", channel_account_id='" + channel_account_id + '\'' +
                ", convert_id=" + convert_id +
                ", channel_campaign_id=" + channel_campaign_id +
                ", ctype=" + ctype +
                ", channel_ad_id=" + channel_ad_id +
                ", click_time=" + click_time +
                ", channel_adgroup_id=" + channel_adgroup_id +
                ", ct=" + ct +
                ", lname='" + lname + '\'' +
                ", utm_campaign='" + utm_campaign + '\'' +
                ", utm_source='" + utm_source + '\'' +
                ", utm_medium='" + utm_medium + '\'' +
                ", utm_term='" + utm_term + '\'' +
                ", utm_content='" + utm_content + '\'' +
                ", channel_ad_name='" + channel_ad_name + '\'' +
                ", channel_campaign_name='" + channel_campaign_name + '\'' +
                ", ip='" + ip + '\'' +
                ", ua='" + ua + '\'' +
                ", mac='" + mac + '\'' +
                ", mac1='" + mac1 + '\'' +
                ", callback_url='" + callback_url + '\'' +
                ", geo='" + geo + '\'' +
                ", imei='" + imei + '\'' +
                ", idfa='" + idfa + '\'' +
                ", idfa_md5='" + idfa_md5 + '\'' +
                ", sl='" + sl + '\'' +
                ", model='" + model + '\'' +
                ", union_site='" + union_site + '\'' +
                ", android_id='" + android_id + '\'' +
                ", oaid='" + oaid + '\'' +
                ", oaid_md5='" + oaid_md5 + '\'' +
                ", channel_adgroup_name='" + channel_adgroup_name + '\'' +
                ", requers_id='" + requers_id + '\'' +
                ", app_key='" + app_key + '\'' +
                ", muid='" + muid + '\'' +
                ", callback='" + callback + '\'' +
                ", channel_type='" + channel_type + '\'' +
                ", push_type='" + push_type + '\'' +
                ", akey='" + akey + '\'' +
                ", token='" + token + '\'' +
                ", channel_keyword_id=" + channel_keyword_id +
                ", qaid_caa='" + qaid_caa + '\'' +
                ", ip_ua_key='" + ip_ua_key + '\'' +
                ", muid_key='" + muid_key + '\'' +
                ", idfa_key='" + idfa_key + '\'' +
                ", imei_key='" + imei_key + '\'' +
                ", android_id_key='" + android_id_key + '\'' +
                ", oaid_key='" + oaid_key + '\'' +
                ", channel_click_id_key='" + channel_click_id_key + '\'' +
                ", company='" + company + '\'' +
                ", auth_account='" + auth_account + '\'' +
                ", hw_app_id='" + hw_app_id + '\'' +
                ", creative_id='" + creative_id + '\'' +
                ", is_delete='" + is_delete + '\'' +
                ", channel_click_id='" + channel_click_id + '\'' +
                '}';
    }
}