package com.dtstack.dtcenter.loader.dto;

import lombok.Builder;
import lombok.Data;

/**
 * SSL 配置相关
 *
 * @author ：wangchuan
 * date：Created in 上午10:47 2021/9/13
 * company: www.dtstack.com
 */
@Data
@Builder
public class SSLConfigDTO {

    /**
     * TLS 验证的方法。共有三种模式：（FULL 默认）CA和NONE
     * 对于FULL，执行正常的 TLS 验证
     * 对于CA，仅验证 CA，但允许主机名不匹配
     * 对于NONE，没有验证
     */
    private String SSLVerification;

    /**
     * 连接到启用了证书身份验证的 Trino 集群时使用。指定PEM或JKS文件的路径
     */
    private String SSLKeyStorePath;

    /**
     * KeyStore 的密码(如果有)
     */
    private String SSLKeyStorePassword;

    /**
     * 密钥库的类型。默认类型由 Java keystore.type安全属性提供
     */
    private String SSLKeyStoreType;

    /**
     * 要使用的 Java TrustStore 文件的位置。验证 HTTPS 服务器证书
     */
    private String SSLTrustStorePath;

    /**
     * TrustStore 的密码
     */
    private String SSLTrustStorePassword;

    /**
     * TrustStore 的类型。默认类型由 Java keystore.type安全属性提供
     */
    private String SSLTrustStoreType;
}
