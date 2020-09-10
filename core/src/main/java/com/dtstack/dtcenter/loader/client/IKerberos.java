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
     * @return
     * @throws Exception
     */
    Map<String, Object> parseKerberosFromUpload(String zipLocation, String localKerberosPath) throws Exception;

    /**
     * 连接 Kerberos 前的准备工作
     * 1. 会替换相对路径到绝对路径，目前支持的是存在一个或者一个 / 都不存在的情况
     * 2. 会增加或者修改一些 Principal 参数
     *
     * @param conf
     * @param localKerberosPath
     * @return
     * @throws Exception
     */
    Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) throws Exception;

    /**
     * 从 JDBC URL 中获取 Principal
     *
     * @param url
     * @return
     * @throws Exception
     */
    String getPrincipals(String url) throws Exception;

    /**
     * 从 Kerberos 配置文件中获取 Principal
     *
     * @param kerberosConfig
     * @return
     * @throws Exception
     */
    List<String> getPrincipals(Map<String, Object> kerberosConfig) throws Exception;
}
