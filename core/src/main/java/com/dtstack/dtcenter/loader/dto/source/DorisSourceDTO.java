package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@ToString
@SuperBuilder
public class DorisSourceDTO extends Mysql5SourceDTO {
    @Override
    public Integer getSourceType() {
        return DataSourceType.DORIS.getVal();
    }
}
