package com.zhugeio.etl.pipeline.model;

import lombok.Data;
import java.io.Serializable;

import com.alibaba.fastjson2.JSON;

@Data
public class DeviceProp implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer id;
    private Integer appId;
    private String name;
    private Integer type;
    private Integer hidden;
    private String owner;

    /**
     * 从JSON字符串转换成DeviceProp对象
     * @param jsonString JSON字符串
     * @return DeviceProp对象
     */
    public static DeviceProp fromJson(String jsonString) {
        return JSON.parseObject(jsonString, DeviceProp.class);
    }

    /**
     * 将对象转换为JSON字符串
     * @return JSON字符串
     */
    public String toJson() {
        return JSON.toJSONString(this);
    }
}