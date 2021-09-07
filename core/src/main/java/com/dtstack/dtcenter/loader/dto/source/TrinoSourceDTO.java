package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * trino sourceDTO
 *
 * @author ：wangchuan
 * date：Created in 下午2:23 2021/9/7
 * company: www.dtstack.com
 */
@ToString
@SuperBuilder
public class TrinoSourceDTO extends RdbmsSourceDTO {

    @Override
    public Integer getSourceType() {
        return DataSourceType.TRINO.getVal();
    }
}
