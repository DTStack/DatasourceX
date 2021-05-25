package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @company: www.dtstack.com
 * @Author ：WangChuan
 * @Date ：Created in 17:54 2020/5/22
 * @Description：Oracle数据源信息
 */
@Data
@ToString
@SuperBuilder
public class OracleSourceDTO extends RdbmsSourceDTO  {


    @Override
    public Integer getSourceType() {
        return DataSourceType.Oracle.getVal();
    }

}
