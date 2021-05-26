package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:21 2020/5/22
 * @Description：DB2 数据源信息
 */
@Data
@ToString
@SuperBuilder
public class Db2SourceDTO extends RdbmsSourceDTO {


    @Override
    public Integer getSourceType() {
        return DataSourceType.DB2.getVal();
    }
}
