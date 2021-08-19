package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:29 2020/5/22
 * @Description：ES 数据源信息
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class ESSourceDTO extends RdbmsSourceDTO {
    /**
     * 其他配置信息
     */
    private String others;

    /**
     * es文档id
     */
    private String id;

    /**
     * ssl 证书绝对路径,只支持.p12 和 .crt 文件
     *
     */
    private String keyPath;


    @Override
    public Integer getSourceType() {
        return DataSourceType.ES.getVal();
    }
}
