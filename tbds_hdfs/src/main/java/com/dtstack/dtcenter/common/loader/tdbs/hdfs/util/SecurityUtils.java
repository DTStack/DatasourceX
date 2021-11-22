package com.dtstack.dtcenter.common.loader.tdbs.hdfs.util;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.loader.common.DtClassThreadFactory;
import com.dtstack.dtcenter.common.loader.tdbs.hdfs.HadoopConfUtil;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import sun.security.krb5.Config;

import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 安全认证工具类, tbds_hdfs 仅支持 tbds 认证
 *
 * @author ：wangchuan
 * date：Created in 下午2:02 2021/11/19
 * company: www.dtstack.com
 */
@Slf4j
public class SecurityUtils {

    private static final String KERBEROS_AUTH = "hadoop.security.authentication";

    private static final String TBDS_AUTH_TYPE = "tbds";

    private static final String TBDS_NAME = "hadoop.security.authentication.tbds_username";

    private static final String TBDS_ID = "hadoop.security.authentication.tbds.secureid";

    private static final String TBDS_KEY = "hadoop.security.authentication.tbds.securekey";

    private static final ConcurrentHashMap<String, UGICacheData> UGI_INFO = new ConcurrentHashMap<>();

    private static final ScheduledExecutorService SCHEDULED_THREAD_POOL_EXECUTOR = new ScheduledThreadPoolExecutor(1, new DtClassThreadFactory("ugiCacheFactory"));

    static {
        SCHEDULED_THREAD_POOL_EXECUTOR.scheduleAtFixedRate(new SecurityUtils.CacheTimerTask(), 0, 10, TimeUnit.SECONDS);
    }

    /**
     * 认证并获取结果
     *
     * @param supplier     Supplier
     * @param hadoopConfig hadoop 配置
     * @param <T>          返回结果范型
     * @return 执行结果
     */
    public static <T> T login(Supplier<T> supplier, String hadoopConfig) {
        JSONObject hadoopJson = JSONObject.parseObject(hadoopConfig);
        Configuration configuration = new Configuration();
        for (String key : hadoopJson.keySet()) {
            configuration.set(key, hadoopJson.getString(key));
        }
        return login(supplier, configuration);
    }

    /**
     * 认证并获取结果
     *
     * @param supplier      Supplier
     * @param configuration 配置类
     * @param <T>           返回结果范型
     * @return 执行结果
     */
    public static <T> T login(Supplier<T> supplier, Configuration configuration) {

        synchronized (DataSourceType.class) {
            // 非 tbds 认证，需要重新刷 UGI 信息
            boolean isTbdsAuth = StringUtils.equalsIgnoreCase(configuration.get(KERBEROS_AUTH), TBDS_AUTH_TYPE);
            if (!isTbdsAuth) {
                try {
                    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                    if (UserGroupInformation.isSecurityEnabled() || !UserGroupInformation.AuthenticationMethod.SIMPLE.equals(currentUser.getAuthenticationMethod())) {
                        Config.refresh();
                        UserGroupInformation.setConfiguration(HadoopConfUtil.getDefaultConfiguration());
                        return currentUser.doAs((PrivilegedExceptionAction<T>) supplier::get);
                    }
                } catch (Exception e) {
                    throw new DtLoaderException(String.format("simple login failed,%s", e.getMessage()), e);
                }
            }

            String tbdsName = configuration.get(TBDS_NAME);
            String tbdsId = configuration.get(TBDS_ID);
            String tbdsKey = configuration.get(TBDS_KEY);
            String ugiCacheName = String.format("%s_%s_%s", tbdsName, tbdsId, tbdsKey);

            // 判断缓存UGI，如果存在则直接使用
            UGICacheData cacheData = UGI_INFO.get(ugiCacheName);
            UserGroupInformation ugi;
            if (cacheData != null) {
                ugi = cacheData.getUgi();
            } else {
                try {
                    // 开始 认证
                    log.info("login start, tbdsName={}, tbdsId={}, tbdsKey={} ", tbdsName, tbdsId, tbdsKey);
                    Config.refresh();
                    UserGroupInformation.setConfiguration(configuration);
                    UserGroupInformation.loginUserFromSubject(null);
                    ugi = UserGroupInformation.getLoginUser();
                    UGI_INFO.put(ugiCacheName, new UGICacheData(ugi));
                    log.info("login success, currentUser={}", UserGroupInformation.getCurrentUser());
                } catch (Exception e) {
                    throw new DtLoaderException(String.format("auth login failed,%s", e.getMessage()), e);
                }
            }
            try {
                return ugi.doAs((PrivilegedExceptionAction<T>) supplier::get);
            } catch (Exception e) {
                throw new DtLoaderException(String.format("ugi doAs failed,%s", e.getMessage()), e);
            }
        }
    }

    static class CacheTimerTask implements Runnable {
        @Override
        public void run() {
            for (String s : UGI_INFO.keySet()) {
                clearKey(s);
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

}
