package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.cache.cp.CpConfig;
import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.Connection;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:06 2020/5/22
 * @Description：关系型数据库 DTO
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class RdbmsSourceDTO implements ISourceDTO {
    /**
     * 用户名
     */
    protected String username;

    /**
     * 密码
     */
    protected String password;

    /**
     * 地址
     */
    protected String url;

    /**
     * 库
     */
    private String schema;

    /**
     * 连接信息
     */
    private Connection connection;

    /**
     * kerberos 配置信息
     */
    private Map<String, Object> kerberosConfig;

    /**
     * 连接池的 KEY，如果不为空，则必填
     */
    private String cpKey;

    /**
     * 连接池配置信息
     */
    private CpConfig cpConfig;

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
