package com.dtstack.dtcenter.common.loader.hdfs.util;

import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:03 2020/9/17
 * @Description：Hdfs Kerberos 认证操作
 */
public class HdfsKerberosLoginUtil extends KerberosLoginUtil {
    /**
     * Hdfs 下载需要设置 RM_Principal
     *
     * @param confMap
     * @return
     */
    public static synchronized UserGroupInformation loginKerberosWithUGI(Map<String, Object> confMap) {
        // 因为 Hdfs 需要下载所以设置 ResourceManager Principal
        confMap.put(HadoopConfTool.RM_PRINCIPAL, confMap.get(HadoopConfTool.PRINCIPAL));
        return loginKerberosWithUGI(confMap, HadoopConfTool.PRINCIPAL, HadoopConfTool.PRINCIPAL_FILE, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);
    }
}
