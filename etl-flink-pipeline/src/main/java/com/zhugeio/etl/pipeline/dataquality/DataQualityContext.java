package com.zhugeio.etl.pipeline.dataquality;

import java.io.Serializable;

/**
 * 数据质量上下文
 */
public class DataQualityContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer appId;
    private Integer platform;
    private String eventName;
    private String eventId;      // $zg_eid
    private String zgZgid;       // $zg_zgid
    private String zgDid;        // $zg_did
    private Long ct;             // $ct
    private String tz;           // $tz
    private String sdk;
    private String pl;
    private String rawJson;

    // Getters & Setters
    public Integer getAppId() { return appId; }
    public void setAppId(Integer appId) { this.appId = appId; }

    public Integer getPlatform() { return platform; }
    public void setPlatform(Integer platform) { this.platform = platform; }

    public String getEventName() { return eventName; }
    public void setEventName(String eventName) { this.eventName = eventName; }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getZgZgid() { return zgZgid; }
    public void setZgZgid(String zgZgid) { this.zgZgid = zgZgid; }

    public String getZgDid() { return zgDid; }
    public void setZgDid(String zgDid) { this.zgDid = zgDid; }

    public Long getCt() { return ct; }
    public void setCt(Long ct) { this.ct = ct; }

    public String getTz() { return tz; }
    public void setTz(String tz) { this.tz = tz; }

    public String getSdk() { return sdk; }
    public void setSdk(String sdk) { this.sdk = sdk; }

    public String getPl() { return pl; }
    public void setPl(String pl) { this.pl = pl; }

    public String getRawJson() { return rawJson; }
    public void setRawJson(String rawJson) { this.rawJson = rawJson; }

    // Builder
    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private DataQualityContext ctx = new DataQualityContext();

        public Builder appId(Integer v) { ctx.appId = v; return this; }
        public Builder platform(Integer v) { ctx.platform = v; return this; }
        public Builder eventName(String v) { ctx.eventName = v; return this; }
        public Builder eventId(String v) { ctx.eventId = v; return this; }
        public Builder zgZgid(String v) { ctx.zgZgid = v; return this; }
        public Builder zgDid(String v) { ctx.zgDid = v; return this; }
        public Builder ct(Long v) { ctx.ct = v; return this; }
        public Builder tz(String v) { ctx.tz = v; return this; }
        public Builder sdk(String v) { ctx.sdk = v; return this; }
        public Builder pl(String v) { ctx.pl = v; return this; }
        public Builder rawJson(String v) { ctx.rawJson = v; return this; }

        public DataQualityContext build() { return ctx; }
    }
}