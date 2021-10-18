package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@ToString
@SuperBuilder
public class TbdsHdfsSourceDTO extends HdfsSourceDTO {

    private String tbdsUsername;

    private String tbdsSecureId;

    private String tbdsSecureKey;

    @Override
    public Integer getSourceType() {
        return DataSourceType.TBDS_HDFS.getVal();
    }

}
