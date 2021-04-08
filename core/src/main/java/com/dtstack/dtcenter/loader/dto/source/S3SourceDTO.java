package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.Connection;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:02 2020/9/29
 * @Description：S3 数据源
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class S3SourceDTO implements ISourceDTO {
    /**
     * 用户名
     */
    protected String username;

    /**
     * 密码
     */
    protected String password;

    /**
     * 数据源类型
     */
    protected Integer sourceType;

    /**
     * 域名信息
     */
    private String hostname;

    @Override
    public Connection getConnection() {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public void setConnection(Connection connection) {
        throw new DtLoaderException("The method is not supported");
    }
}
