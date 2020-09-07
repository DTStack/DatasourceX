package com.dtstack.dtcenter.common.loader.impala;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.hadoop.DtKerberosUtils;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Matcher;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:16 2020/1/7
 * @Description：Impala 工厂连接
 */
@Slf4j
public class ImpalaConnFactory extends ConnFactory {
    public ImpalaConnFactory() {
        this.driverName = DataBaseType.Impala.getDriverClassName();
    }

    @Override
    public Connection getConn(ISourceDTO iSource) throws Exception {
        init();
        ImpalaSourceDTO impalaSourceDTO = (ImpalaSourceDTO) iSource;
        Connection connection = null;
        if (MapUtils.isNotEmpty(impalaSourceDTO.getKerberosConfig())) {
            String principalFile = (String) impalaSourceDTO.getKerberosConfig().get("principalFile");
            String principal = (String) impalaSourceDTO.getKerberosConfig().get("principal");
            log.info("getKuduClient principal {},principalFile:{}", principal, principalFile);
            connection = KerberosUtil.loginKerberosWithUGI(impalaSourceDTO.getKerberosConfig()).doAs(
                    (PrivilegedAction<Connection>) () -> {
                        try {
                            return super.getConn(impalaSourceDTO);
                        } catch (Exception e) {
                            throw new DtCenterDefException("getImpalaConnection error : " + e.getMessage(), e);
                        }
                    }
            );
        } else {
            connection = super.getConn(impalaSourceDTO);
        }
        String db = StringUtils.isBlank(impalaSourceDTO.getSchema()) ? getImpalaSchema(impalaSourceDTO.getUrl()) : impalaSourceDTO.getSchema();
        if (StringUtils.isNotBlank(db)) {
            DBUtil.executeSqlWithoutResultSet(connection, String.format(DtClassConsistent.PublicConsistent.USE_DB, db),
                    false);
        }
        return connection;
    }

    /**
     * 获取 Impala schema
     *
     * @param jdbcUrl
     * @return
     */
    private String getImpalaSchema(String jdbcUrl) {
        Matcher matcher = DtClassConsistent.PatternConsistent.IMPALA_JDBC_PATTERN.matcher(jdbcUrl);
        String db = "";
        if (matcher.matches()) {
            db = matcher.group(1);
        }
        return db;
    }
}
