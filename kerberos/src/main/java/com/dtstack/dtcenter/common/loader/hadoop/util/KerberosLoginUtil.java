package com.dtstack.dtcenter.common.loader.hadoop.util;

import com.dtstack.dtcenter.common.loader.common.DtClassThreadFactory;
import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HadoopConfUtil;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import sun.security.krb5.Config;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:59 2020/9/1
 * @Description：Kerberos 登录相关操作
 */
@Slf4j
public class KerberosLoginUtil {
    /**
     * Kerberos 默认角色配置信息
     */
    private static final String SECURITY_TO_LOCAL = "hadoop.security.auth_to_local";
    private static final String SECURITY_TO_LOCAL_DEFAULT = "RULE:[1:$1] RULE:[2:$1]";

    private static ConcurrentHashMap<String, UGICacheData> UGI_INFO = new ConcurrentHashMap<>();

    private static final ScheduledExecutorService scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1, new DtClassThreadFactory("ugiCacheFactory"));

    static {
        scheduledThreadPoolExecutor.scheduleAtFixedRate(new KerberosLoginUtil.CacheTimerTask(), 0, 10, TimeUnit.SECONDS);
    }

    static class CacheTimerTask implements Runnable {
        @Override
        public void run() {
            Iterator<String> iterator = UGI_INFO.keySet().iterator();
            while (iterator.hasNext()) {
                clearKey(iterator.next());
            }
        }

        private void clearKey(String principal) {
            UGICacheData ugiCacheData = UGI_INFO.get(principal);
            if (ugiCacheData == null || ugiCacheData.getUgi() == null) {
                UGI_INFO.remove(principal);
                log.info("KerberosLogin CLEAR UGI {}", principal);
                return;
            }

            if (System.currentTimeMillis() > ugiCacheData.getTimeoutStamp()) {
                UGI_INFO.remove(principal);
                log.info("KerberosLogin CLEAR UGI {}", principal);
            }
        }
    }

    public static synchronized UserGroupInformation loginWithUGI(Map<String, Object> confMap) {
        return loginWithUGI(confMap, HadoopConfTool.PRINCIPAL, HadoopConfTool.PRINCIPAL_FILE, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);
    }

    /**
     * 需要从 jdbcURL 中获取 Principal 信息，通过这个信息去登录系统
     *
     * @param jdbcUrl
     * @param confMap
     * @return
     */
    public static synchronized UserGroupInformation loginWithUGI(String jdbcUrl, Map<String, Object> confMap) {
        String principal = KerberosConfigUtil.getPrincipalFromUrl(jdbcUrl);
        confMap.put(HadoopConfTool.PRINCIPAL, principal);
        return loginWithUGI(confMap, HadoopConfTool.PRINCIPAL, HadoopConfTool.PRINCIPAL_FILE, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);
    }

    public static synchronized UserGroupInformation loginWithUGI(Map<String, Object> confMap, String principal, String keytab, String krb5Conf) {
        // 非 Kerberos 认证，需要重新刷 UGI 信息
        if (MapUtils.isEmpty(confMap)) {
            try {
                UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                if (UserGroupInformation.isSecurityEnabled() || !UserGroupInformation.AuthenticationMethod.SIMPLE.equals(currentUser.getAuthenticationMethod())) {
                    Config.refresh();
                    UserGroupInformation.setConfiguration(HadoopConfUtil.getDefaultConfiguration());
                }
                return currentUser;
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
            principal = KerberosConfigUtil.getPrincipals(keytab).get(0);
        }
        // 校验 Principal 和 Keytab 文件
        if (StringUtils.isEmpty(principal) || StringUtils.isEmpty(keytab)) {
            throw new DtLoaderException("Kerberos Login fail, principal or keytab is null");
        }

        // 因为 Hive 需要下载，所有优先设置 ResourceManager Principal
        if (confMap.get(HadoopConfTool.RM_PRINCIPAL) == null) {
            confMap.put(HadoopConfTool.RM_PRINCIPAL, principal);
        }

        // 处理 Default 角色
        if (MapUtils.getString(confMap, SECURITY_TO_LOCAL) == null || "DEFAULT".equals(MapUtils.getString(confMap, SECURITY_TO_LOCAL))) {
            confMap.put(SECURITY_TO_LOCAL, SECURITY_TO_LOCAL_DEFAULT);
        }

        // 判断缓存UGI，如果存在则直接使用
        UGICacheData cacheData = UGI_INFO.get(principal + "_" + keytab);
        if (cacheData != null) {
            return cacheData.getUgi();
        }

        try {
            // 设置 Krb5 配置文件
            if (StringUtils.isNotEmpty(krb5Conf)) {
                System.setProperty(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, krb5Conf);
            }

            // 开始 Kerberos 认证
            log.info("login kerberos, currentUser={}, principal={}, principalFilePath={}, krb5ConfPath={}", UserGroupInformation.getCurrentUser(), principal, keytab, krb5Conf);
            Config.refresh();
            Configuration config = KerberosConfigUtil.getConfig(confMap);
            config.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(config);
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
            UGI_INFO.put(principal + "_" + keytab, new UGICacheData(ugi));
            log.info("login kerberos success, currentUser={}", UserGroupInformation.getCurrentUser());
            return ugi;
        } catch (Exception var6) {
            throw new DtLoaderException("login kerberos failed", var6);
        }
    }
}
