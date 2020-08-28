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
    /**
     * 从 ZIP 包中解压出 Kerberos 配置信息
     * 其中地址相关信息因为 SFTP 的原因，只存储相对路径，在校验之前再做转化
     * 调用 #{@link #prepareKerberosForConnect}
     *
     * @param zipLocation
     * @param localKerberosPath
     * @param datasourceType
     * @return
     * @throws Exception
     */
    Map<String, String> parseKerberosFromUpload(String zipLocation, String localKerberosPath, Integer datasourceType) throws Exception;

    /**
     * 连接 Kerberos 前的准备工作
     *
     * @param conf
     * @param localKerberosPath
     * @param datasourceType
     * @return
     * @throws Exception
     */
    Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath, Integer datasourceType) throws Exception;

    /**
     * 从 JDBC URL 中获取 Principal
     *
     * @param url
     * @param datasourceType
     * @return
     * @throws Exception
     */
    String getPrincipal(String url, Integer datasourceType) throws Exception;

    /**
     * 从 Kerberos 配置文件中获取 Principal
     *
     * @param kerberosConfig
     * @param datasourceType
     * @return
     * @throws Exception
     */
    List<String> getPrincipal(Map<String, Object> kerberosConfig, Integer datasourceType) throws Exception;
}
