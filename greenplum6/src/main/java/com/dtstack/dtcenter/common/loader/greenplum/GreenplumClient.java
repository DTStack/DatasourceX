package com.dtstack.dtcenter.common.loader.greenplum;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Greenplum6SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import org.apache.commons.lang.StringUtils;

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

    private static final String TABLE_QUERY = "SELECT relname from pg_class a,pg_namespace b where relname not like " +
            "'%%prt%%' and relkind ='r'  and a.relnamespace=b.oid and  nspname = '%s';";

    private static final String TABLE_QUERY_WITHOUT_SCHEMA = "\n" +
            "select table_schema ||'.'||table_name as tableName from information_schema.tables where table_schema in " +
            "(SELECT n.nspname AS \"Name\"  FROM pg_catalog.pg_namespace n WHERE n.nspname !~ '^pg_' AND n.nspname <>" +
            " 'gp_toolkit' AND n.nspname <> 'information_schema' ORDER BY 1)";

    private static final String TABLE_COMMENT_QUERY = "select de.description\n" +
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
            "         where 1=1 and tab.relname='%s' and tab.nspname = '%s'";

    private static final String DATABASE_QUERY = "select nspname from pg_namespace";

    @Override
    protected ConnFactory getConnFactory() {
        return new GreenplumFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.GREENPLUM6;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        Greenplum6SourceDTO greenplum6SourceDTO = (Greenplum6SourceDTO) iSource;

        // 校验 schema，特殊处理表名中带了 schema 信息的
        String tableName = queryDTO.getTableName();
        String schema = greenplum6SourceDTO.getSchema();
        if (StringUtils.isEmpty(greenplum6SourceDTO.getSchema())) {
            if (!queryDTO.getTableName().contains(".")) {
                throw new DtCenterDefException("greenplum数据源需要schema参数");
            }
            schema = queryDTO.getTableName().split("\\.")[0];
            tableName = queryDTO.getTableName().split("\\.")[1];
        }

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = greenplum6SourceDTO.getConnection().createStatement();
            resultSet = statement.executeQuery(String.format(TABLE_COMMENT_QUERY, tableName, schema));
            while (resultSet.next()) {
                String tableDesc = resultSet.getString(1);
                return tableDesc;
            }
        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, greenplum6SourceDTO.clearAfterGetConnection(clearStatus));
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
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        Greenplum6SourceDTO greenplum6SourceDTO = (Greenplum6SourceDTO) iSource;

        Statement statement = null;
        ResultSet resultSet = null;
        List<String> tableList = new ArrayList<>();
        try {
            statement = greenplum6SourceDTO.getConnection().createStatement();
            if (StringUtils.isBlank(greenplum6SourceDTO.getSchema())) {
                resultSet = statement.executeQuery(TABLE_QUERY_WITHOUT_SCHEMA);
            } else {
                resultSet = statement.executeQuery(String.format(TABLE_QUERY, greenplum6SourceDTO.getSchema()));
            }

            while (resultSet.next()) {
                tableList.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    greenplum6SourceDTO.getSchema()),
                    DBErrorCode.GET_TABLE_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, greenplum6SourceDTO.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Greenplum6SourceDTO greenplum6SourceDTO = (Greenplum6SourceDTO) source;
        GreenplumDownloader greenplumDownloader = new GreenplumDownloader(getCon(greenplum6SourceDTO),
                queryDTO.getSql(), greenplum6SourceDTO.getSchema());
        greenplumDownloader.configure();
        return greenplumDownloader;
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        queryDTO.setSql(DATABASE_QUERY);
        return super.getAllDatabases(source, queryDTO);
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
