package com.dtstack.dtcenter.loader.dto.source;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:54 2020/5/22
 * @Description：Hive1 数据源信息
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class Hive1SourceDTO extends RdbmsSourceDTO {
    /**
     * Hadoop defaultFS
     */
    @Builder.Default
    private String defaultFS = "";

    /**
     * Hadoop/ Hbase 配置信息
     */
    private String config;
}
