package com.dtstack.dtcenter.loader.dto.source;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * hive 开启ssl认证参数
 *
 * @author luming
 * @date 2022/1/4
 */
@AllArgsConstructor
@Data
public class HiveSslConfig {
    /**
     * 是否开启ssl
     */
    private Boolean useSsl;

    /**
     * 密钥库路径
     */
    private String keystorePath;

    /**
     * 密钥库密码
     */
    private String keystorePassword;
}
