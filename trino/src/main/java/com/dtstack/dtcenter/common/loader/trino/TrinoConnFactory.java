package com.dtstack.dtcenter.common.loader.trino;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.PropertyUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosConfigUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.SSLConfigDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.TrinoSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import sun.security.krb5.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * trino 数据源连接工厂类
 *
 * @author ：wangchuan
 * date：Created in 下午2:21 2021/9/9
 * company: www.dtstack.com
 */
@Slf4j
public class TrinoConnFactory extends ConnFactory {

    /**
     * Kerberos 服务名称
     */
    public static final String KERBEROS_REMOTE_SERVICE_NAME = "KerberosRemoteServiceName";

    /**
     * principal
     */
    public static final String KERBEROS_PRINCIPAL = "KerberosPrincipal";

    /**
     * keytab 路径
     */
    public static final String KERBEROS_KEYTAB_PATH = "KerberosKeytabPath";

    public TrinoConnFactory() {
        driverName = DataBaseType.TRINO.getDriverClassName();
        this.errorPattern = new TrinoErrorPattern();
        testSql = DataBaseType.TRINO.getTestSql();
    }

    @Override
    public Connection getConn(ISourceDTO sourceDTO, String taskParams) throws Exception {
        init();
        TrinoSourceDTO trinoSourceDTO = (TrinoSourceDTO) sourceDTO;
        try {
            Properties properties = new Properties();
            // 处理 ssl
            buildSSLConfig(properties, trinoSourceDTO.getSslConfigDTO());
            DriverManager.setLoginTimeout(30);
            // kerberos
            Map<String, Object> kerberosConfig = trinoSourceDTO.getKerberosConfig();
            PropertyUtil.putIfNotNull(properties, DtClassConsistent.PublicConsistent.USER, trinoSourceDTO.getUsername());
            PropertyUtil.putIfNotNull(properties, DtClassConsistent.PublicConsistent.PASSWORD, trinoSourceDTO.getPassword());
            if (MapUtils.isEmpty(kerberosConfig)) {
                // 未开启 kerberos 直接返回
                return getConnAndSetSchema(trinoSourceDTO.getUrl(), trinoSourceDTO.getSchema(), properties);
            }
            // 处理 kerberos
            buildKerberosConfig(properties, kerberosConfig);
            // 加锁，防止其他线程正在进行 kerberos 认证
            synchronized (DataSourceType.class) {
                // 不在 properties 设置，不一致会 SpnegoHandler 报错
                System.setProperty(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, MapUtils.getString(kerberosConfig, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF));
                Config.refresh();
                return getConnAndSetSchema(trinoSourceDTO.getUrl(), trinoSourceDTO.getSchema(), properties);
            }
        } catch (SQLException e) {
            // 对异常进行统一处理
            throw new DtLoaderException(errorAdapter.connAdapter(e.getMessage(), errorPattern), e);
        }
    }

    /**
     * 处理 SSL 认证信息
     *
     * @param properties   prop 配置
     * @param sslConfigDTO ssl 配置
     */
    private void buildSSLConfig(Properties properties, SSLConfigDTO sslConfigDTO) {
        if (Objects.nonNull(sslConfigDTO)) {
            // 设置 ssl 参数
            PropertyUtil.putIfNotNull(properties, DtClassConsistent.SSLConsistent.SSL, "true");
            PropertyUtil.putIfNotNull(properties, DtClassConsistent.SSLConsistent.SSL_VERIFICATION, sslConfigDTO.getSSLVerification());
            PropertyUtil.putIfNotNull(properties, DtClassConsistent.SSLConsistent.SSL_KEYSTORE_PATH, sslConfigDTO.getSSLKeyStorePath());
            PropertyUtil.putIfNotNull(properties, DtClassConsistent.SSLConsistent.SSL_KEYSTORE_PASSWORD, sslConfigDTO.getSSLKeyStorePassword());
            PropertyUtil.putIfNotNull(properties, DtClassConsistent.SSLConsistent.SSL_KEYSTORE_TYPE, sslConfigDTO.getSSLKeyStoreType());
            PropertyUtil.putIfNotNull(properties, DtClassConsistent.SSLConsistent.SSL_TRUST_STORE_PATH, sslConfigDTO.getSSLTrustStorePath());
            PropertyUtil.putIfNotNull(properties, DtClassConsistent.SSLConsistent.SSL_TRUSTSTORE_PASSWORD, sslConfigDTO.getSSLTrustStoreType());
            PropertyUtil.putIfNotNull(properties, DtClassConsistent.SSLConsistent.SSL_TRUSTSTORE_TYPE, sslConfigDTO.getSSLTrustStoreType());
        }
    }

    /**
     * 处理 kerberos 配置
     *
     * @param properties     prop 配置
     * @param kerberosConfig kerberos 配置
     */
    private void buildKerberosConfig(Properties properties, Map<String, Object> kerberosConfig) {
        String keytabPath = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL_FILE);
        AssertUtils.notBlank(keytabPath, "keytab path can't be null");
        String principal = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL);
        if (StringUtils.isBlank(principal)) {
            List<String> principals = KerberosConfigUtil.getPrincipals(keytabPath);
            if (CollectionUtils.isEmpty(principals)) {
                throw new DtLoaderException(String.format("The principal parsed from keytab [%s] is empty", keytabPath));
            }
            // 取第一个 principal 账号
            principal = principals.get(0);
        }
        // 设置 kerberos 参数
        PropertyUtil.putIfNotNull(properties, KERBEROS_PRINCIPAL, principal);
        PropertyUtil.putIfNotNull(properties, KERBEROS_KEYTAB_PATH, keytabPath);
        PropertyUtil.putIfNotNull(properties, DtClassConsistent.PublicConsistent.USER, principal);
        // 默认 kerberos 服务名为 trino，不设置会报错
        String KerberosRemoteServiceName = MapUtils.getString(kerberosConfig, KERBEROS_REMOTE_SERVICE_NAME, "trino");
        PropertyUtil.putIfNotNull(properties, KERBEROS_REMOTE_SERVICE_NAME, KerberosRemoteServiceName);
        log.info("kerberos properties info:\nprincipal:{}\nkeytabPath:{}\nKerberosRemoteServiceName:{}\n", principal, keytabPath, KERBEROS_REMOTE_SERVICE_NAME);
    }

    /**
     * 获取 connection 并设置 schema
     *
     * @param url        url 信息
     * @param schema     schema
     * @param properties prop 配置
     * @return 设置 schema 后的 connection
     */
    private Connection getConnAndSetSchema(String url, String schema, Properties properties) throws SQLException {
        AssertUtils.notBlank(url, "jdbc url can't be empty");
        // 未开启 kerberos 直接返回
        Connection connection = DriverManager.getConnection(url, properties);
        setSchema(connection, schema);
        return connection;
    }
}
