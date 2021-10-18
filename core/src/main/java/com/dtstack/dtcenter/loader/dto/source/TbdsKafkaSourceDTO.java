package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;


@Data
@ToString
@SuperBuilder
public class TbdsKafkaSourceDTO extends KafkaSourceDTO {

    private String secureId;

    private String secureKey;


    @Override
    public Integer getSourceType() {
        return DataSourceType.TBDS_KAFKA.getVal();
    }

}
