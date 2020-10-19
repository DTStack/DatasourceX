package com.dtstack.dtcenter.common.loader.hive;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.hive.util.HiveKerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
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
        this.driverName = DataBaseType.HIVE.getDriverClassName();
    }

    @Override
    public Connection getConn(ISourceDTO iSource) throws Exception {
        init();
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;

        Connection connection;
        if (MapUtils.isNotEmpty(hiveSourceDTO.getKerberosConfig())) {
            connection = HiveKerberosLoginUtil.loginKerberosWithUGI(hiveSourceDTO.getUrl(), hiveSourceDTO.getKerberosConfig()).doAs(
                    (PrivilegedAction<Connection>) () -> {
                        try {
                            return super.getConn(hiveSourceDTO);
                        } catch (Exception e) {
                            throw new DtLoaderException("getHiveConnection error : " + e.getMessage(), e);
                        }
                    }
            );
        } else {
            connection = super.getConn(hiveSourceDTO);
        }

        Matcher matcher = DtClassConsistent.PatternConsistent.HIVE_JDBC_PATTERN.matcher(hiveSourceDTO.getUrl());
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
