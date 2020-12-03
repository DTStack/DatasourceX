package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.common.hadoop.DtKerberosUtils;
import com.dtstack.dtcenter.common.hadoop.HadoopConfTool;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
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
 * @Date ：Created in 09:58 2020/8/4
 * @Description：Kerberos 工具类
 */
@Slf4j
public class KerberosUtil {
    private static final String SECURITY_TO_LOCAL = "hadoop.security.auth_to_local";
    private static final String SECURITY_TO_LOCAL_DEFAULT = "RULE:[1:$1] RULE:[2:$1]";

    public static synchronized UserGroupInformation loginWithUGI(Map<String, Object> confMap) {
        return loginWithUGI(confMap, "principal", "principalFile", "java.security.krb5.conf");
    }

    public static synchronized UserGroupInformation loginWithUGI(Map<String, Object> confMap, String principal, String keytab, String krb5Conf) {
        // 非 Kerberos 认证，需要重新刷 UGI 信息
        if (MapUtils.isEmpty(confMap)) {
            try {
                Config.refresh();
                UserGroupInformation.setConfiguration(new Configuration());
                return UserGroupInformation.getCurrentUser();
            } catch (Exception e) {
                throw new DtLoaderException("simple login failed", e);
            }
        }

        //Kerberos 认证属性
        principal = MapUtils.getString(confMap, principal);
        keytab = MapUtils.getString(confMap, keytab);
        krb5Conf = MapUtils.getString(confMap, krb5Conf);
        // 兼容历史逻辑
        if (StringUtils.isNotEmpty(keytab) && !keytab.contains("/")) {
            keytab = MapUtils.getString(confMap, "keytabPath");
        }
        // 如果前端没传 Principal 则直接从 Keytab 中获取第一个 Principal
        if (StringUtils.isEmpty(principal) && StringUtils.isNotEmpty(keytab)) {
            principal = DtKerberosUtils.getPrincipal(keytab);
        }

        // 手动替换 Principal 参数，临时方案
        String hbaseMasterPrincipal = MapUtils.getString(confMap, HadoopConfTool.KEY_HBASE_MASTER_KERBEROS_PRINCIPAL);
        if (StringUtils.isNotBlank(hbaseMasterPrincipal) && StringUtils.isNotBlank(principal) && hbaseMasterPrincipal.contains("/") && principal.contains("/")) {
            int hbaseMasterPrincipalLos = hbaseMasterPrincipal.indexOf("/");
            int principalLos = principal.indexOf("/");
            principal = principal.replaceFirst(principal.substring(0, principalLos), hbaseMasterPrincipal.substring(0, hbaseMasterPrincipalLos));
        }
        // 校验 Principal 和 Keytab 文件
        if (StringUtils.isEmpty(principal) || StringUtils.isEmpty(keytab)) {
            throw new DtLoaderException("Kerberos Login fail, principal or keytab is null");
        }

        // 处理 Default 角色
        if (MapUtils.getString(confMap, SECURITY_TO_LOCAL) == null || "DEFAULT".equals(MapUtils.getString(confMap, SECURITY_TO_LOCAL))) {
            confMap.put(SECURITY_TO_LOCAL, SECURITY_TO_LOCAL_DEFAULT);
        }

        try {
            // 设置 Krb5 配置文件
            if (StringUtils.isNotEmpty(krb5Conf)) {
                System.setProperty(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, krb5Conf);
            }

            // 开始 Kerberos 认证
            log.info("login kerberos, currentUser={}, principal={}, principalFilePath={}, krb5ConfPath={}", UserGroupInformation.getCurrentUser(), principal, keytab, krb5Conf);
            Config.refresh();
            Configuration config = DtKerberosUtils.getConfig(confMap);
            config.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(config);
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
            log.info("login kerberos success, currentUser={}", UserGroupInformation.getCurrentUser());
            return ugi;
        } catch (Exception var6) {
            log.error("login kerberos failed, config:{}", confMap, var6);
            throw new DtLoaderException("login kerberos failed", var6);
        }
    }
}
