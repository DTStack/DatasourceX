package com.dtstack.dtcenter.loader.dto.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:51 2020/5/22
 * @Description：HDFS 数据源信息
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class HdfsSourceDTO extends RdbmsSourceDTO {
    /**
     * Hadoop defaultFS
     */
    private String defaultFS = "";

    /**
     * Hadoop/ Hbase 配置信息
     */
    private String config;

    // TODO 临时处理 yarn 日志下载
    private String appIdStr;

    private int readLimit = 4095;

    private String logType = null;

    private Map<String, Object> yarnConf;
}
