package com.dtstack.dtcenter.common.loader.hive1;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.hadoop.DtKerberosUtils;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.regex.Matcher;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/1/7
 * @Description：Hive 连接池工厂
 */
@Slf4j
public class HiveConnFactory extends ConnFactory {
    public HiveConnFactory() {
        this.driverName = DataBaseType.HIVE1X.getDriverClassName();
        this.testSql = DataBaseType.HIVE1X.getTestSql();
    }

    @Override
    public Connection getConn(SourceDTO source) throws Exception {
        init();
        DriverManager.setLoginTimeout(30);
        Configuration conf = null;
        if (MapUtils.isNotEmpty(source.getKerberosConfig())) {
            String principalFile = (String) source.getKerberosConfig().get("principalFile");
            log.info("getHiveConnection principalFile:{}", principalFile);
            conf = DtKerberosUtils.loginKerberos(source.getKerberosConfig());
        }

        Matcher matcher = DtClassConsistent.PatternConsistent.HIVE_JDBC_PATTERN.matcher(source.getUrl());
        Connection connection = DriverManager.getConnection(source.getUrl(), source.getUsername(), source.getPassword());
        String db = null;
        if (!matcher.find()) {
            db = matcher.group(DtClassConsistent.PublicConsistent.DB_KEY);
        }
        db = StringUtils.isBlank(source.getSchema()) ? db : source.getSchema();
        if (StringUtils.isNotEmpty(db)) {
            DBUtil.executeSqlWithoutResultSet(connection, String.format(DtClassConsistent.PublicConsistent.USE_DB, db), false);
        }
        return connection;
    }
}
