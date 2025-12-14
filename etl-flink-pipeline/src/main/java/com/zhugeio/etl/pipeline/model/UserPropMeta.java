package com.zhugeio.etl.pipeline.model;

import lombok.Data;
import java.io.Serializable;

import com.alibaba.fastjson2.JSON;

@Data
public class UserPropMeta implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer id;
    private Integer appId;
    private String name;
    private Integer type = 1;
    private Integer isMultiple = 0;
    private Integer hidden = 0;
    private String owner = "zg";
    private Integer encryptionType = 0;
    private String aliasName = "";
    private Integer attrType = 0;
    private String sqlRule;
    private String sqlJson;
    private Integer isStop = 0;
    private String tableFields;
    private Integer isDelete = 0;
    private Integer appointType = 0;
    private Integer creatorId;
    private String creatorName;
    private Long createdTime;
    private Long updatedTime;

    /**
     * 从JSON字符串转换成UserPropMeta对象
     * @param jsonString JSON字符串
     * @return UserPropMeta对象
     */
    public static UserPropMeta fromJson(String jsonString) {
        return JSON.parseObject(jsonString, UserPropMeta.class);
    }

    /**
     * 将对象转换为JSON字符串
     * @return JSON字符串
     */
    public String toJson() {
        return JSON.toJSONString(this);
    }
}