

package com.dtstack.dtcenter.common.loader.hdfs;

import java.util.Locale;

/**
 * Orc 字段类型转换器
 *
 * @author ：wangchuan
 * date：Created in 下午8:39 2021/11/22
 * company: www.dtstack.com
 */
public class OrcColumnTypeConverter {

    /**
     * @param type 原始数据类型
     * @return 转化后的数据类型
     */
    public static String apply(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "INT":
            case "INTEGER":
            case "SMALLINT":
            case "TINYINT":
                return "int";
            case "BINARY":
                return "binary";
            case "BIGINT":
                return "bigint";
            case "BOOLEAN":
                return "boolean";
            case "FLOAT":
                return "float";
            case "DOUBLE":
                return "double";
            case "DATE":
                return "date";
            case "TIMESTAMP":
                return "timestamp";
            default:
                if (type.contains("DECIMAL")) {
                    return type.toLowerCase();
                }
                return "string";
        }
    }
}
