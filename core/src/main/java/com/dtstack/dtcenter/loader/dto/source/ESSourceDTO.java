package com.dtstack.dtcenter.loader.dto.source;

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
}
