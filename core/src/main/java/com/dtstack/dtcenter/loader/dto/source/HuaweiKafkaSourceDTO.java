package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;


@Data
@ToString
@SuperBuilder
public class HuaweiKafkaSourceDTO extends KafkaSourceDTO {

    @Override
    public Integer getSourceType() {
        return DataSourceType.HUAWEI_KAFKA.getVal();
    }
}
