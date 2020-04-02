package com.dtstack.dtcenter.common.loader.rdbms.hive;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.hadoop.DtKerberosUtils;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Matcher;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/1/7
 * @Description：Hive 连接池工厂
 */
public class HiveConnFactory extends ConnFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HiveConnFactory.class);

    public HiveConnFactory() {
        this.driverName = DataBaseType.HIVE.getDriverClassName();
    }


    @Override
    public Connection getConn(SourceDTO source) throws Exception {
        init();
        Configuration conf = null;
        if (MapUtils.isNotEmpty(source.getKerberosConfig())) {
            String principalFile = (String) source.getKerberosConfig().get("principalFile");
            LOG.info("getHiveConnection principalFile:{}", principalFile);

            conf = DtKerberosUtils.loginKerberos(source.getKerberosConfig());
            //拼接URL
            //url = concatHiveJdbcUrl(conf, url);
        }

        Matcher matcher = DtClassConsistent.PatternConsistent.HIVE_JDBC_PATTERN.matcher(source.getUrl());
        String db = null;
        String host = null;
        String port = null;
        String param = null;
        if (matcher.find()) {
            host = matcher.group(DtClassConsistent.PublicConsistent.HOST_KEY);
            port = matcher.group(DtClassConsistent.PublicConsistent.PORT_KEY);
            db = matcher.group(DtClassConsistent.PublicConsistent.DB_KEY);
            param = matcher.group(DtClassConsistent.PublicConsistent.PARAM_KEY);
        }

        if (StringUtils.isNotEmpty(host)) {
            param = param == null ? "" : param;
            String url = String.format("jdbc:hive2://%s:%s/%s", host, port, param);
            Connection connection = DriverManager.getConnection(url, source.getUsername(), source.getPassword());
            if (StringUtils.isNotEmpty(db)) {
                try {
                    connection.createStatement().execute(String.format(DtClassConsistent.PublicConsistent.USE_DB, db));
                } catch (SQLException e) {
                    if (connection != null) {
                        connection.close();
                    }

                    if (e.getMessage().contains("NoSuchDatabaseException")) {
                        throw new DtCenterDefException(e.getMessage(), DBErrorCode.DB_NOT_EXISTS);
                    } else {
                        throw e;
                    }
                }
            }

            return connection;
        }

        throw new DtCenterDefException("jdbcUrl 不规范");
    }
}
