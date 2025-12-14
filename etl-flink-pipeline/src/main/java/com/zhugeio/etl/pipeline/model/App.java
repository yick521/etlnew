package com.zhugeio.etl.pipeline.model;

import lombok.Data;
import java.io.Serializable;
import java.util.Date;

import com.alibaba.fastjson2.JSON;

@Data
public class App implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private Integer id;
    private Integer mainId;
    private Integer sdkPlatform;
    private String configInfo;
    private Integer hasData;
    private Date creatTime;
    private Integer isDelete;
    private Integer eventSum;
    private Integer attrSum;
    private Date server06AppUpdateTime;
    private Date server06UpdateTime;
    private Date server58AppUpdateTime;
    private Date server58UpdateTime;
    private Date zhugeioinfUpdateTime;
    private Date zhugeioinfAppUpdateTime;
    private Date server177UpdateTime;
    private Date server177AppUpdateTime;
    private Date infobrightUpdateTime;
    private Date infobrightAppUpdateTime;
    private Date infobright4UpdateTime;
    private Date infobright4AppUpdateTime;
    private Date infobright3UpdateTime;
    private Date infobright3AppUpdateTime;
    private Date hasDataTime;
    private String etlLoadLevel;
    
    /**
     * 从JSON字符串转换成App对象
     * @param jsonString JSON字符串
     * @return App对象
     */
    public static App fromJson(String jsonString) {
        return JSON.parseObject(jsonString, App.class);
    }
    
    /**
     * 将对象转换为JSON字符串
     * @return JSON字符串
     */
    public String toJson() {
        return JSON.toJSONString(this);
    }
}