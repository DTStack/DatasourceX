package com.dtstack.dtcenter.loader.enums;

/**
 *
 * hdfsWriter支持的格式
 *
 * @author ：wangchuan
 * date：Created in 上午10:44 2020/8/11
 * company: www.dtstack.com
 */
public enum FileFormat {

    ORC("orc"),

    TEXT("text"),

    PARQUET("parquet");

    private String val;

    FileFormat(String val) {
        this.val = val;
    }

    public String getVal() {
        return val;
    }
}
