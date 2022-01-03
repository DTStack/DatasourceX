package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@ToString
@SuperBuilder
public class SapHana2SourceDTO extends SapHanaSourceDTO {

    @Override
    public Integer getSourceType() {
        return DataSourceType.SAP_HANA2.getVal();
    }

}
