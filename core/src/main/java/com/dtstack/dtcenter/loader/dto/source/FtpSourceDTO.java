package com.dtstack.dtcenter.loader.dto.source;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:31 2020/5/22
 * @Description：FTP 数据源信息
 */
@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FtpSourceDTO implements ISourceDTO {
    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 是否缓存
     */
    @Builder.Default
    protected Boolean isCache = false;

    /**
     * 地址
     */
    private String url;

    /**
     * 端口号
     */
    private String hostPort;

    /**
     * 协议
     */
    private String protocol;

    /**
     * 认证
     */
    private String auth;

    /**
     * 目录
     * FTP rsa 路径
     */
    private String path;

    /**
     * 连接模式
     */
    private String connectMode;
}
