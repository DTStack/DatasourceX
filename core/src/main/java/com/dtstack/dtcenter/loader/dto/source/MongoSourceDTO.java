package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @company: www.dtstack.com
 * @Author ：WangChuan
 * @Date ：Created in 19:20 2020/5/22
 * @Description：Mongo 数据源信息
 */
@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MongoSourceDTO  implements ISourceDTO {

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 数据源类型
     */
    protected Integer sourceType;

    /**
     * 端口号
     */
    private String hostPort;

    /**
     * 模式即 DBName
     */
    private String schema;

    /**
     * 如果为 master slave 的则为 master 的地址
     */
    private String master;

    /**
     * 连接池配置信息，如果传入则认为开启连接池
     */
    private PoolConfig poolConfig;
}
