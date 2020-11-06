package com.dtstack.dtcenter.common.loader.hive1;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.hive1.util.HiveKerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.Hive1SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.security.PrivilegedAction;
import java.sql.Connection;
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
        this.errorPattern = new Hive1ErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource) throws Exception {
        init();
        Hive1SourceDTO hive1SourceDTO = (Hive1SourceDTO) iSource;

        Connection connection;
        if (MapUtils.isNotEmpty(hive1SourceDTO.getKerberosConfig())) {
            connection = HiveKerberosLoginUtil.loginKerberosWithUGI(hive1SourceDTO.getUrl(), hive1SourceDTO.getKerberosConfig()).doAs(
                    (PrivilegedAction<Connection>) () -> {
                        try {
                            return super.getConn(hive1SourceDTO);
                        } catch (Exception e) {
                            throw new DtLoaderException("getHiveConnection error : " + e.getMessage(), e);
                        }
                    }
            );
        } else {
            connection = super.getConn(hive1SourceDTO);
        }

        Matcher matcher = DtClassConsistent.PatternConsistent.HIVE_JDBC_PATTERN.matcher(hive1SourceDTO.getUrl());
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
