package com.dtstack.dtcenter.loader.dto.source;


import com.dtstack.dtcenter.loader.enums.RedisMode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:26 2020/5/22
 * @Description：Redis 数据源信息
 */
@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisSourceDTO implements ISourceDTO {
    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 端口号
     */
    private String hostPort;

    /**
     * 模式即 DBName
     */
    private String schema;

    /**
     * Redis 部署模式
     */
    private RedisMode redisMode;

    /**
     * 如果为 master slave 的则为 master 的地址
     */
    private String master;


}
