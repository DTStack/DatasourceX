package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.sql.Connection;

/**
 * open TSDB 数据源连接信息
 *
 * @author ：wangchuan
 * date：Created in 上午10:43 2021/6/17
 * company: www.dtstack.com
 */
@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class OpenTSDBSourceDTO implements ISourceDTO {

    /**
     * url
     */
    private String url;

    @Override
    public Integer getSourceType() {
        return DataSourceType.OPENTSDB.getVal();
    }

    @Override
    public Connection getConnection() {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public void setConnection(Connection connection) {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public String getUsername() {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public String getPassword() {
        throw new DtLoaderException("The method is not supported");
    }
}
