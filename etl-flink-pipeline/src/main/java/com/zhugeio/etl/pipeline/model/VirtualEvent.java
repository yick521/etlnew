package com.zhugeio.etl.pipeline.model;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import java.util.Date;

/**
 * 虚拟事件实体类
 * 对应数据库表: virtual_event
 */
public class VirtualEvent {
    private int id;
    private String eventName;
    private String aliasName;
    private int appId;
    private String description;
    private Date stopDateTime;
    private String eventJson;
    private boolean hidden;
    private int isDelete;
    private int isStop;
    private int eventStatus;
    private String createUser;
    private Date insertTime;
    private Date updateTime;
    private String bindEvent;
    private String bindEventAttr;
    private Date lastInsertTime;
    private Integer markType;
    private String owner;
    private Integer groupId;

    public VirtualEvent() {
    }

    public static VirtualEvent fromJson(String json) {
        return JSON.parseObject(json, VirtualEvent.class);
    }

    public String toJson() {
        return JSON.toJSONString(this);
    }

    // Getters and setters
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getStopDateTime() {
        return stopDateTime;
    }

    public void setStopDateTime(Date stopDateTime) {
        this.stopDateTime = stopDateTime;
    }

    public String getEventJson() {
        return eventJson;
    }

    public void setEventJson(String eventJson) {
        this.eventJson = eventJson;
    }

    public boolean isHidden() {
        return hidden;
    }

    public void setHidden(boolean hidden) {
        this.hidden = hidden;
    }

    public int getIsDelete() {
        return isDelete;
    }

    public void setIsDelete(int isDelete) {
        this.isDelete = isDelete;
    }

    public int getIsStop() {
        return isStop;
    }

    public void setIsStop(int isStop) {
        this.isStop = isStop;
    }

    public int getEventStatus() {
        return eventStatus;
    }

    public void setEventStatus(int eventStatus) {
        this.eventStatus = eventStatus;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public Date getInsertTime() {
        return insertTime;
    }

    public void setInsertTime(Date insertTime) {
        this.insertTime = insertTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getBindEvent() {
        return bindEvent;
    }

    public void setBindEvent(String bindEvent) {
        this.bindEvent = bindEvent;
    }

    public String getBindEventAttr() {
        return bindEventAttr;
    }

    public void setBindEventAttr(String bindEventAttr) {
        this.bindEventAttr = bindEventAttr;
    }

    public Date getLastInsertTime() {
        return lastInsertTime;
    }

    public void setLastInsertTime(Date lastInsertTime) {
        this.lastInsertTime = lastInsertTime;
    }

    public Integer getMarkType() {
        return markType;
    }

    public void setMarkType(Integer markType) {
        this.markType = markType;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Integer getGroupId() {
        return groupId;
    }

    public void setGroupId(Integer groupId) {
        this.groupId = groupId;
    }

    @Override
    public String toString() {
        return "VirtualEvent{" +
                "id=" + id +
                ", eventName='" + eventName + '\'' +
                ", aliasName='" + aliasName + '\'' +
                ", appId=" + appId +
                ", description='" + description + '\'' +
                ", stopDateTime=" + stopDateTime +
                ", eventJson='" + eventJson + '\'' +
                ", hidden=" + hidden +
                ", isDelete=" + isDelete +
                ", isStop=" + isStop +
                ", eventStatus=" + eventStatus +
                ", createUser='" + createUser + '\'' +
                ", insertTime=" + insertTime +
                ", updateTime=" + updateTime +
                ", bindEvent='" + bindEvent + '\'' +
                ", bindEventAttr='" + bindEventAttr + '\'' +
                ", lastInsertTime=" + lastInsertTime +
                ", markType=" + markType +
                ", owner='" + owner + '\'' +
                ", groupId=" + groupId +
                '}';
    }
}