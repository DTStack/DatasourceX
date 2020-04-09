package com.dtstack.dtcenter.common.loader.rdbms.impala;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.regex.Matcher;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:16 2020/1/7
 * @Description：Impala 工厂连接
 */
public class ImpalaCoonFactory extends ConnFactory {
    public ImpalaCoonFactory() {
        this.driverName = DataBaseType.Impala.getDriverClassName();
    }

    @Override
    public Connection getConn(SourceDTO source) throws Exception {
        Connection conn = super.getConn(source);
        String db = StringUtils.isBlank(source.getSchema()) ? getImpalaSchema(source.getUrl()) : source.getSchema();
        if (StringUtils.isNotBlank(db)) {
            DBUtil.executeSqlWithoutResultSet(conn, String.format(DtClassConsistent.PublicConsistent.USE_DB, db), false);
        }
        return conn;
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
        if(matcher.matches()){
            db = matcher.group(1);
        }
        return db;
    }
}
