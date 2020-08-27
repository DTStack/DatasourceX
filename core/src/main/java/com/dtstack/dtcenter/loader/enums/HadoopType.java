package com.dtstack.dtcenter.loader.enums;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:59 2020/8/27
 * @Description：
 */
public enum HadoopType {
    ;

    private Integer sourceType;

    private String hadoopPlugin;

    HadoopType(Integer sourceType, String hadoopPlugin) {
        this.sourceType = sourceType;
        this.hadoopPlugin = hadoopPlugin;
    }

    public static String getHadoopPlugin(Integer sourceType) {
        for (HadoopType value : HadoopType.values()) {
            if (value.sourceType.equals(sourceType)) {
                return value.hadoopPlugin;
            }
        }

        return "hadoop2";
    }
}
