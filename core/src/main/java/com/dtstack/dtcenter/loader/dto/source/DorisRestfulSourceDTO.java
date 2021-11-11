package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@ToString
@SuperBuilder
public class DorisRestfulSourceDTO extends RestfulSourceDTO {

    /**
     * 集群名称
     */
    private String cluster;

    /**
     * 库
     */

    private String schema;

    private String userName;

    private String password;

    @Override
    public String getUsername() {
        return userName;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public Integer getSourceType() {
        return DataSourceType.DorisRestful.getVal();
    }
}
