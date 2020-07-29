package com.dtstack.dtcenter.common.loader.hive1;

import com.dtstack.dtcenter.common.hadoop.DtKerberosUtils;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.source.Hive1SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.source.DataBaseType;
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
    public Connection getConn(ISourceDTO iSource) throws Exception {
        init();
        Hive1SourceDTO hive1SourceDTO = (Hive1SourceDTO) iSource;

        DriverManager.setLoginTimeout(30);
        Configuration conf = null;
        if (MapUtils.isNotEmpty(hive1SourceDTO.getKerberosConfig())) {
            String principalFile = (String) hive1SourceDTO.getKerberosConfig().get("principalFile");
            log.info("getHiveConnection principalFile:{}", principalFile);
            conf = DtKerberosUtils.loginKerberos(hive1SourceDTO.getKerberosConfig());
        }

        Matcher matcher = DtClassConsistent.PatternConsistent.HIVE_JDBC_PATTERN.matcher(hive1SourceDTO.getUrl());
        Connection connection = DriverManager.getConnection(hive1SourceDTO.getUrl(), hive1SourceDTO.getUsername(),
                hive1SourceDTO.getPassword());
        String db = null;
        if (!matcher.find()) {
            db = matcher.group(DtClassConsistent.PublicConsistent.DB_KEY);
        }
        db = StringUtils.isBlank(hive1SourceDTO.getSchema()) ? db : hive1SourceDTO.getSchema();
        if (StringUtils.isNotEmpty(db)) {
            DBUtil.executeSqlWithoutResultSet(connection, String.format(DtClassConsistent.PublicConsistent.USE_DB,
                    db), false);
        }
        return connection;
    }
}