package com.dtstack.dtcenter.common.loader.kudu;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import sun.security.krb5.Config;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 09:58 2020/8/4
 * @Description：Kerberos 工具类
 */
@Slf4j
public class KerberosUtil {

    public static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    public static synchronized UserGroupInformation loginKerberosWithUGI(Map<String, Object> confMap) {
        return loginKerberosWithUGI(confMap, "principal", "principalFile", "java.security.krb5.conf");
    }

    public static synchronized UserGroupInformation loginKerberosWithUGI(Map<String, Object> confMap, String principal, String keytab, String krb5Conf) {
        principal = MapUtils.getString(confMap, principal);
        keytab = MapUtils.getString(confMap, keytab);
        krb5Conf = MapUtils.getString(confMap, krb5Conf);
        // 兼容历史逻辑
        if (StringUtils.isNotEmpty(keytab) && !keytab.contains("/")) {
            keytab = MapUtils.getString(confMap, "keytabPath");
        }
        // 如果前端没传 Principal 则直接从 Keytab 中获取第一个 Principal
        if (StringUtils.isEmpty(principal) && StringUtils.isNotEmpty(keytab)) {
            principal = getPrincipals(keytab).get(0);
        }
        // 校验 Principal 和 Keytab 文件
        if (StringUtils.isEmpty(principal) || StringUtils.isEmpty(keytab)) {
            throw new DtLoaderException("Kerberos Login fail, principal or keytab is null");
        }

        try {
            // 设置 Krb5 配置文件
            if (StringUtils.isNotEmpty(krb5Conf)) {
                System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, krb5Conf);
            }

            // 开始 Kerberos 认证
            log.info("login kerberos, currentUser={}, principal={}, principalFilePath={}, krb5ConfPath={}", UserGroupInformation.getCurrentUser(), principal, keytab, krb5Conf);
            Config.refresh();
            Configuration config = getConfig(confMap);
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

    /**
     * 从 Keytab 中获取 Principal 信息
     *
     * @param keytabPath
     * @return
     */
    public static List<String> getPrincipals(String keytabPath) {
        File file = new File(keytabPath);
        Keytab keytab = null;

        try {
            keytab = Keytab.loadKeytab(file);
        } catch (IOException e) {
            throw new DtLoaderException("解析 keytab 文件失败", e);
        }

        if (CollectionUtils.isEmpty(keytab.getPrincipals())) {
            throw new DtLoaderException("keytab 中的 Principal 为空");
        }

        return keytab.getPrincipals().stream().map(PrincipalName::getName).collect(Collectors.toList());
    }

    /**
     * 将 Map 转换为 Configuration
     *
     * @param configMap
     * @return
     */
    public static Configuration getConfig(Map<String, Object> configMap) {
        Configuration conf = new Configuration(false);
        Iterator var2 = configMap.entrySet().iterator();

        while (var2.hasNext()) {
            Map.Entry<String, Object> entry = (Map.Entry) var2.next();
            if (entry.getValue() != null && !(entry.getValue() instanceof Map)) {
                conf.set(entry.getKey(), entry.getValue().toString());
            }
        }

        return conf;
    }

}
