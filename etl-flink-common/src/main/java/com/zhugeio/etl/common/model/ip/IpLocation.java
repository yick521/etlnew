package com.zhugeio.etl.common.model.ip;

import java.io.Serializable;

/**
 * IP地理位置解析结果
 */
public class IpLocation implements Serializable {
    private static final long serialVersionUID = 1L;

    private String country;      // 国家
    private String province;     // 省份
    private String city;         // 城市
    private String isp;          // 运营商
    private String ipAddress;    // 原始IP地址

    public IpLocation() {
    }

    public IpLocation(String ipAddress, String country, String province, String city) {
        this.ipAddress = ipAddress;
        this.country = country;
        this.province = province;
        this.city = city;
    }

    // Getters and Setters
    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }

    public String getProvince() { return province; }
    public void setProvince(String province) { this.province = province; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getIsp() { return isp; }
    public void setIsp(String isp) { this.isp = isp; }

    public String getIpAddress() { return ipAddress; }
    public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }

    public boolean isEmpty() {
        return (country == null || country.isEmpty()) &&
                (province == null || province.isEmpty()) &&
                (city == null || city.isEmpty());
    }

    @Override
    public String toString() {
        return String.format("IpLocation{ip='%s', country='%s', province='%s', city='%s', isp='%s'}",
                ipAddress, country, province, city, isp);
    }
}