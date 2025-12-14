package com.zhugeio.etl.pipeline.model;

import lombok.Data;
import java.io.Serializable;
import java.util.Date;

import com.alibaba.fastjson2.JSON;

@Data
public class CompanyApp implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private Integer id;
    private String appName;
    private Integer appType;
    private String appKey;
    private String pushKey;
    private Integer createUserId;
    private Integer companyId;
    private Date createDateTime;
    private Date updateDateTime;
    private Integer eventSum;
    private Integer attrSum;
    private Integer isDelete;
    private Integer stop;
    private String appAccount;
    private String warehouse;
    private String warehouseR;
    private Integer autoEvent;
    private Integer autoStrategy;
    private Integer autoLabel;
    private Byte appVersion;
    private String type;
    private String typeName;
    private String description;
    private Long projectId;
    private Integer appDefault;
    private String unit;
    
    /**
     * 从JSON字符串转换成CompanyApp对象
     * @param jsonString JSON字符串
     * @return CompanyApp对象
     */
    public static CompanyApp fromJson(String jsonString) {
        return JSON.parseObject(jsonString, CompanyApp.class);
    }
    
    /**
     * 将对象转换为JSON字符串
     * @return JSON字符串
     */
    public String toJson() {
        return JSON.toJSONString(this);
    }
}