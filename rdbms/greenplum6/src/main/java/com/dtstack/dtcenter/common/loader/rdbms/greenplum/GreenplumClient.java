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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:11 2020/4/10
 * @Description：Greenplum 客户端
 */
public class GreenplumClient extends AbsRdbmsClient {

    private static final String TABLE_COLUMN_QUERY = "select des.description from information_schema.columns col" +
            " left join pg_description des on col.table_name::regclass = des.objoid" +
            " and col.ordinal_position = des.objsubid where table_schema = '%s' and table_name = '%s'";

    private static final String TABLE_QUERY = "select c.relname as tablename" +
            " from pg_catalog.pg_class c, pg_catalog.pg_namespace n" +
            " where" +
            " n.oid = c.relnamespace" +
            " and n.nspname='%s'";

    private static final String TABLE_COMMENT_QUERY="select de.description\n" +
            "          from (select pc.oid as ooid,pn.nspname,pc.*\n" +
            "      from pg_class pc\n" +
            "           left outer join pg_namespace pn\n" +
            "                        on pc.relnamespace = pn.oid\n" +
            "      where 1=1\n" +
            "       and pc.relkind in ('r')\n" +
            "       and pn.nspname not in ('pg_catalog','information_schema')\n" +
            "       and pn.nspname not like 'pg_toast%%'\n" +
            "       and pc.oid not in (\n" +
            "          select inhrelid\n" +
            "            from pg_inherits\n" +
            "       )\n" +
            "       and pc.relname not like '%%peiyb%%'\n" +
            "    order by pc.relname) tab\n" +
            "               left outer join (select pd.*\n" +
            "     from pg_description pd\n" +
            "    where 1=1\n" +
            "      and pd.objsubid = 0) de\n" +
            "                            on tab.ooid = de.objoid\n" +
            "         where 1=1 and tab.relname='%s'";


    @Override
    protected ConnFactory getConnFactory() {
        return new GreenplumFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.GREENPLUM6;
    }

    @Override
    public String  getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(source, queryDTO);
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = source.getConnection().createStatement();
            resultSet = statement.executeQuery(String.format(TABLE_COMMENT_QUERY, queryDTO.getTableName()));
            while (resultSet.next()) {
                String tableDesc = resultSet.getString(1);
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


    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeQuery(source, queryDTO, false);
        Statement statement = null;
        ResultSet resultSet = null;
        List<String> tableList = new ArrayList<>();
        try {
            statement = source.getConnection().createStatement();
            resultSet=statement.executeQuery(String.format(TABLE_QUERY,source.getSchema()));
            while (resultSet.next()) {
                tableList.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    source.getSchema()),
                    DBErrorCode.GET_TABLE_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, source.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

}
