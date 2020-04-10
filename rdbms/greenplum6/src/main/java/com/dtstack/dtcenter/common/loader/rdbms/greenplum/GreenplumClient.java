package com.dtstack.dtcenter.common.loader.rdbms.greenplum;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Matcher;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:11 2020/4/10
 * @Description：Greenplum 客户端
 */
public class GreenplumClient extends AbsRdbmsClient {

    private static final String TABLE_DESC_QUERY = "select des.description from information_schema.columns col" +
            " left join pg_description des on col.table_name::regclass = des.objoid" +
            " and col.ordinal_position = des.objsubid where table_schema = '%s' and table_name = '%s'";

    @Override
    protected ConnFactory getConnFactory() {
        return new GreenplumFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.GREENPLUM6;
    }

    @Override
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(source, queryDTO);
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            String dbName = source.getSchema();
            if (StringUtils.isBlank(dbName)) {
                dbName = getGreenplumDbFromJdbc(source.getUrl());
            }

            statement = source.getConnection().createStatement();
            resultSet = statement.executeQuery(String.format(TABLE_DESC_QUERY, dbName, queryDTO.getTableName()));
            while (resultSet.next()) {
                String tableDesc = resultSet.getString(0);
                return tableDesc;
            }
        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, source.clearAfterGetConnection(clearStatus));
        }
        return "";
    }

    private static String getGreenplumDbFromJdbc(String jdbcUrl) {
        if (StringUtils.isEmpty(jdbcUrl)) {
            return null;
        }
        Matcher matcher = DtClassConsistent.PatternConsistent.GREENPLUM_JDBC_PATTERN.matcher(jdbcUrl);
        String db = "";
        if (matcher.matches()) {
            db = matcher.group(1);
        }
        return db;
    }
}
