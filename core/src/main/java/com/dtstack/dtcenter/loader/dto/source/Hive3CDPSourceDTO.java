package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * hive3-cdp sourceDTO
 *
 * @author ：wangchuan
 * date：Created in 下午8:29 2021/12/13
 * company: www.dtstack.com
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class Hive3CDPSourceDTO extends RdbmsSourceDTO {
    /**
     * Hadoop defaultFS
     */
    @Builder.Default
    private String defaultFS = "";

    /**
     * metastore
     */
    private String metastore;

    /**
     * Hadoop/ Hbase 配置信息
     */
    private String config;

    /**
     * hive ssl
     */
    private HiveSslConfig hiveSslConfig;


    @Override
    public Integer getSourceType() {
        return DataSourceType.HIVE3_CDP.getVal();
    }
}
