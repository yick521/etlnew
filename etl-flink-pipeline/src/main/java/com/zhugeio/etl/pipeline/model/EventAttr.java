package com.zhugeio.etl.pipeline.model;

import lombok.Data;
import java.io.Serializable;

import com.alibaba.fastjson2.JSON;

@Data
public class EventAttr implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer attrId;
    private Integer eventId;
    private String attrName;
    private Integer propType = 1;
    private Integer isStop = 0;
    private Integer hidden = 0;
    private String owner = "zg";
    private String columnName;
    private String aliasName = "";
    private Integer attrType = 0;
    private String sqlJson;
    private Integer isDelete = 0;
    // 注意：这个字段在数据库中不存在，是从关联查询中获得的
    private Integer appId;
    // 注意：这个字段在数据库中不存在，是从关联查询中获得的
    private String eventName;

    /**
     * 从JSON字符串转换成EventAttr对象
     * @param jsonString JSON字符串
     * @return EventAttr对象
     */
    public static EventAttr fromJson(String jsonString) {
        return JSON.parseObject(jsonString, EventAttr.class);
    }

    /**
     * 将对象转换为JSON字符串
     * @return JSON字符串
     */
    public String toJson() {
        return JSON.toJSONString(this);
    }
}