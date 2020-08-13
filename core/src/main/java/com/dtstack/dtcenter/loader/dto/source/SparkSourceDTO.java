package com.dtstack.dtcenter.loader.dto.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 09:51 2020/8/13
 * @Description：Spark 数据源信息
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class SparkSourceDTO extends RdbmsSourceDTO {
    /**
     * Hadoop defaultFS
     */
    private String defaultFS = "";

    /**
     * Hadoop/ Hbase 配置信息
     */
    private String config;
}
