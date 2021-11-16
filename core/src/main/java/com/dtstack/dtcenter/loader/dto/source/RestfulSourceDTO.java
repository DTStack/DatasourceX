package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.Connection;
import java.util.Map;

/**
 * restful 数据源信息
 *
 * @author ：wangchuan
 * date：Created in 下午2:00 2021/8/9
 * company: www.dtstack.com
 */
@Data
@ToString
@SuperBuilder
public class RestfulSourceDTO implements ISourceDTO {

    /**
     * 请求地址
     */
    private String url;

    /**
     * 协议 仅支持 HTTP/HTTPS
     */
    private String protocol;

    /**
     * 请求头信息
     */
    private Map<String, String> headers;

    /**
     * 连接超时时间，单位：秒
     */
    private Integer connectTimeout;

    /**
     * socket 超时时间，单位：秒
     */
    private Integer socketTimeout;

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

    @Override
    public Integer getSourceType() {
        return DataSourceType.RESTFUL.getVal();
    }
}
