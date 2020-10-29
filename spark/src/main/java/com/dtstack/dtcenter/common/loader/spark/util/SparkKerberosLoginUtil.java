package com.dtstack.dtcenter.common.loader.spark.util;

import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:03 2020/9/11
 * @Description：Spark Kerberos 认证
 */
public class SparkKerberosLoginUtil extends KerberosLoginUtil {
    /**
     * 需要从 jdbcURL 中获取 Principal 信息，通过这个信息去登录系统
     *
     * @param jdbcUrl
     * @param confMap
     * @return
     */
    public static synchronized UserGroupInformation loginKerberosWithUGI(String jdbcUrl, Map<String, Object> confMap) {
        return loginKerberosWithUGI(confMap, HadoopConfTool.PRINCIPAL, HadoopConfTool.PRINCIPAL_FILE, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);
    }
}
