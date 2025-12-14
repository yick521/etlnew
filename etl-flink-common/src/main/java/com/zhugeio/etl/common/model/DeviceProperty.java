package com.zhugeio.etl.common.model;

import java.io.Serializable;

/**
 * 设备属性信息
 */
public class DeviceProperty implements Serializable {
    private static final long serialVersionUID = 1L;

    // 标准化后的设备信息
    private String standardBrand;        // 标准化品牌名: Huawei, Xiaomi, Apple
    private String standardModel;        // 标准化型号: Mate 40 Pro, iPhone 14
    private String deviceCategory;       // 设备分类: Flagship, Mid-range, Entry-level
    private String screenSize;           // 屏幕尺寸: 6.1", 6.7"
    private String ram;                  // 内存: 8GB, 12GB
    private String releaseYear;          // 发布年份: 2023, 2022

    // 设备特征
    private boolean is5GSupported;       // 是否支持5G
    private boolean isFoldable;          // 是否为折叠屏
    private String priceRange;           // 价格区间: Premium, Mid, Budget

    // 原始设备信息
    private String originalBrand;
    private String originalModel;

    public DeviceProperty() {
    }

    public DeviceProperty(String originalBrand, String originalModel) {
        this.originalBrand = originalBrand;
        this.originalModel = originalModel;
    }

    // Getters and Setters

    public String getStandardBrand() {
        return standardBrand;
    }

    public void setStandardBrand(String standardBrand) {
        this.standardBrand = standardBrand;
    }

    public String getStandardModel() {
        return standardModel;
    }

    public void setStandardModel(String standardModel) {
        this.standardModel = standardModel;
    }

    public String getDeviceCategory() {
        return deviceCategory;
    }

    public void setDeviceCategory(String deviceCategory) {
        this.deviceCategory = deviceCategory;
    }

    public String getScreenSize() {
        return screenSize;
    }

    public void setScreenSize(String screenSize) {
        this.screenSize = screenSize;
    }

    public String getRam() {
        return ram;
    }

    public void setRam(String ram) {
        this.ram = ram;
    }

    public String getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(String releaseYear) {
        this.releaseYear = releaseYear;
    }

    public boolean is5GSupported() {
        return is5GSupported;
    }

    public void set5GSupported(boolean is5GSupported) {
        this.is5GSupported = is5GSupported;
    }

    public boolean isFoldable() {
        return isFoldable;
    }

    public void setFoldable(boolean foldable) {
        isFoldable = foldable;
    }

    public String getPriceRange() {
        return priceRange;
    }

    public void setPriceRange(String priceRange) {
        this.priceRange = priceRange;
    }

    public String getOriginalBrand() {
        return originalBrand;
    }

    public void setOriginalBrand(String originalBrand) {
        this.originalBrand = originalBrand;
    }

    public String getOriginalModel() {
        return originalModel;
    }

    public void setOriginalModel(String originalModel) {
        this.originalModel = originalModel;
    }

    @Override
    public String toString() {
        return String.format("DeviceProperty{brand='%s', model='%s', category='%s', priceRange='%s'}",
                standardBrand, standardModel, deviceCategory, priceRange);
    }
}
