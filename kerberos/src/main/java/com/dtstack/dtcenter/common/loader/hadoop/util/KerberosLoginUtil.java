package com.dtstack.dtcenter.common.loader.hadoop.util;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import sun.security.krb5.Config;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:59 2020/9/1
 * @Description：Kerberos 登录相关操作
 */
@Slf4j
public class KerberosLoginUtil {
    public static synchronized UserGroupInformation loginKerberosWithUGI(Map<String, Object> confMap) {
        return loginKerberosWithUGI(confMap, HadoopConfTool.PRINCIPAL, HadoopConfTool.PRINCIPAL_FILE, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);
    }

    /**
     * 需要从 jdbcURL 中获取 Principal 信息，通过这个信息去登录系统
     *
     * @param jdbcUrl
     * @param confMap
     * @return
     */
    public static synchronized UserGroupInformation loginKerberosWithUGI(String jdbcUrl, Map<String, Object> confMap) {
        String principal = KerberosConfigUtil.getPrincipalFromUrl(jdbcUrl);
        confMap.put(HadoopConfTool.PRINCIPAL, principal);
        return loginKerberosWithUGI(confMap, HadoopConfTool.PRINCIPAL, HadoopConfTool.PRINCIPAL_FILE, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);
    }

    public static synchronized UserGroupInformation loginKerberosWithUGI(Map<String, Object> confMap, String principal, String keytab, String krb5Conf) {
        // 替换 _host 信息
        KerberosConfigUtil.replaceHost(confMap);
        principal = MapUtils.getString(confMap, principal);
        keytab = MapUtils.getString(confMap, keytab);
        krb5Conf = MapUtils.getString(confMap, krb5Conf);
        // 兼容历史逻辑
        if (StringUtils.isNotEmpty(keytab) && !keytab.contains("/")) {
            keytab = MapUtils.getString(confMap, "keytabPath");
        }
        // 如果前端没传 Principal 则直接从 Keytab 中获取第一个 Principal
        if (StringUtils.isEmpty(principal) && StringUtils.isNotEmpty(keytab)) {
            principal = KerberosConfigUtil.getPrincipals(keytab).get(0);
        }
        // 校验 Principal 和 Keytab 文件
        if (StringUtils.isEmpty(principal) || StringUtils.isEmpty(keytab)) {
            throw new DtLoaderException("Kerberos Login fail, principal or keytab is null");
        }

        try {
            // 设置 Krb5 配置文件
            if (StringUtils.isNotEmpty(krb5Conf)) {
                System.setProperty(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, krb5Conf);
            }

            // 开始 Kerberos 认证
            log.info("login kerberos, currentUser={}, principal={}, path={}, krb5Conf={}", UserGroupInformation.getCurrentUser(), principal, keytab, krb5Conf);
            Config.refresh();
            Configuration config = KerberosConfigUtil.getConfig(confMap);
            config.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(config);
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
            log.info("login kerberos success, currentUser={}", UserGroupInformation.getCurrentUser());
            return ugi;
        } catch (Exception var6) {
            log.error("login kerberos failed, config:{}", confMap);
            throw new DtLoaderException("login kerberos failed", var6);
        }
    }
}
