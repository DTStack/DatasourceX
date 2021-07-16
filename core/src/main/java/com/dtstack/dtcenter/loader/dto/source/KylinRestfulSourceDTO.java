package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.sql.Connection;

@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class KylinRestfulSourceDTO implements ISourceDTO {

    private String project;
    /**
     * url
     */
    private String url;

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
        return DataSourceType.KylinRestful.getVal();
    }

    @Override
    public Connection getConnection() {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public void setConnection(Connection connection) {
        throw new DtLoaderException("The method is not supported");
    }
}
