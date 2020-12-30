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
 * socket数据源连接信息
 *
 * @author ：wangchuan
 * date：Created in 4:16 下午 2020/12/28
 * company: www.dtstack.com
 */
@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SocketSourceDTO implements ISourceDTO {
    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * socket ip和端口
     */
    private String hostPort;

    /**
     * 数据源类型
     */
    @Builder.Default
    protected Integer sourceType = DataSourceType.SOCKET.getVal();;

    @Override
    public Connection getConnection() {
        throw new DtLoaderException("不支持该方法");
    }

    @Override
    public void setConnection(Connection connection) {
        throw new DtLoaderException("不支持该方法");
    }
}
