package com.dtstack.dtcenter.loader.dto.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:57 2020/7/16
 * @Description：Hbase2 数据源信息
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class Hbase2SourceDTO extends RdbmsSourceDTO {
    /**
     * 目录
     * Hbase 根目录
     */
    private String path;

    /**
     * Hbase 配置信息
     */
    private String config;

    /**
     * 其他配置信息
     */
    private String others;
}
