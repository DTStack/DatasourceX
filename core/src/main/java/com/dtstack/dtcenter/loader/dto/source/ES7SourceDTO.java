package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@ToString
@SuperBuilder
@NoArgsConstructor
public class ES7SourceDTO extends ESSourceDTO {
    @Override
    public Integer getSourceType() {
        return DataSourceType.ES7.getVal();
    }
}
