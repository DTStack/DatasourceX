package com.dtstack.dtcenter.common.loader.hive.util;

import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:45 2020/9/11
 * @Description：Hive Kerberos 认证操作
 */
public class HiveKerberosLoginUtil extends KerberosLoginUtil {
    /**
     * 需要从 jdbcURL 中获取 Principal 信息，通过这个信息去登录系统
     *
     * @param jdbcUrl
     * @param confMap
     * @return
     */
    public static synchronized UserGroupInformation loginWithUGI(String jdbcUrl, Map<String, Object> confMap) {
        // 因为 Hive 需要下载所以设置 ResourceManager Principal
        confMap.put(HadoopConfTool.RM_PRINCIPAL, confMap.get(HadoopConfTool.PRINCIPAL));
        return loginWithUGI(confMap, HadoopConfTool.PRINCIPAL, HadoopConfTool.PRINCIPAL_FILE, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);
    }
}
