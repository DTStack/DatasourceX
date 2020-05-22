package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import lombok.*;

import java.sql.Connection;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 18:24 2020/5/22
 * @Description：Phoenix 数据源信息
 */
@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PhoenixSourceDTO implements ISourceDTO {

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 数据库持续性连接
     */
    private Connection connection;

    /**
     * URL 地址
     */
    private String url;

    /**
     * 获取连接并清除当前对象中的连接
     *
     * @param clearStatus
     * @return
     */
    public Connection clearAfterGetConnection(Integer clearStatus) {
        if (ConnectionClearStatus.NORMAL.getValue().equals(clearStatus)) {
            return this.connection;
        }

        Connection temp = connection;
        this.connection = null;

        if (ConnectionClearStatus.CLEAR.getValue().equals(clearStatus)) {
            return null;
        }
        return temp;
    }

}
