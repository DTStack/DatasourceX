package com.dtstack.dtcenter.loader.dto.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:59 2020/5/22
 * @Description：Hive 数据源信息
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class HiveSourceDTO extends RdbmsSourceDTO {
    /**
     * Hadoop defaultFS
     */
    private String defaultFS = "";

    /**
     * Hadoop/ Hbase 配置信息
     */
    private String config;
}
