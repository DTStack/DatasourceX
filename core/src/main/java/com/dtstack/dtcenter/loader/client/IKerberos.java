package com.dtstack.dtcenter.loader.client;

import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:15 2020/8/25
 * @Description：Kerberos 操作类
 */
public interface IKerberos {
    Map<String, Object> kerberosConfig

    /**
     * 从 JDBC URL 中获取 Principal
     *
     * @param url
     * @return
     */
    String getPrincipal(String url);

    /**
     * 从 Kerberos 配置文件中获取 Principal
     *
     * @param kerberosConfig
     * @return
     */
    List<String> getPrincipal(Map<String, Object> kerberosConfig);
}
