package com.zhugeio.etl.pipeline.model;

import lombok.Data;
import java.io.Serializable;

import com.alibaba.fastjson2.JSON;

@Data
public class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer id;
    private String eventName;
    private Integer appId;
    private Integer isDelete = 0;
    private Integer isStop = 0;
    private String owner = "zg";
    private String aliasName;

    /**
     * 从JSON字符串转换成Event对象
     * @param jsonString JSON字符串
     * @return Event对象
     */
    public static Event fromJson(String jsonString) {
        return JSON.parseObject(jsonString, Event.class);
    }

    /**
     * 将对象转换为JSON字符串
     * @return JSON字符串
     */
    public String toJson() {
        return JSON.toJSONString(this);
    }
}