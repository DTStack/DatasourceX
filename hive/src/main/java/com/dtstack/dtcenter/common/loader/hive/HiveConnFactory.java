package com.dtstack.dtcenter.common.loader.hive;

import com.dtstack.dtcenter.common.hadoop.DtKerberosUtils;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
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
        this.driverName = DataBaseType.HIVE.getDriverClassName();
    }

    @Override
    public Connection getConn(ISourceDTO iSource) throws Exception {
        init();
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;
        DriverManager.setLoginTimeout(30);
        Configuration conf = null;
        if (MapUtils.isNotEmpty(hiveSourceDTO.getKerberosConfig())) {
            String principalFile = (String) hiveSourceDTO.getKerberosConfig().get("principalFile");
            log.info("getHiveConnection principalFile:{}", principalFile);
            conf = DtKerberosUtils.loginKerberos(hiveSourceDTO.getKerberosConfig());
        }

        Matcher matcher = DtClassConsistent.PatternConsistent.HIVE_JDBC_PATTERN.matcher(hiveSourceDTO.getUrl());
        Connection connection = DriverManager.getConnection(hiveSourceDTO.getUrl(), hiveSourceDTO.getUsername(),
                hiveSourceDTO.getPassword());
        String db = null;
        if (!matcher.find()) {
            db = matcher.group(DtClassConsistent.PublicConsistent.DB_KEY);
        }
        db = StringUtils.isBlank(hiveSourceDTO.getSchema()) ? db : hiveSourceDTO.getSchema();
        if (StringUtils.isNotEmpty(db)) {
            DBUtil.executeSqlWithoutResultSet(connection, String.format(DtClassConsistent.PublicConsistent.USE_DB,
                    db), false);
        }
        return connection;
    }
}
