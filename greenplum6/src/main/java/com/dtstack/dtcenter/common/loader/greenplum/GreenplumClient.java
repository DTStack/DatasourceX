package com.dtstack.dtcenter.common.loader.greenplum;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Greenplum6SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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

    private static final String CREATE_SCHEMA_SQL_TMPL = "create schema %s";

    // 判断db是否存在
    private static final String DATABASE_IS_EXISTS = "select nspname from pg_namespace where nspname = '%s'";

    private static final String TABLES_IS_IN_SCHEMA = "select table_name from information_schema.tables WHERE table_schema = '%s' and table_name = '%s'";

    // 获取正在使用数据库
    private static final String CURRENT_DB = "select current_database()";

    @Override
    protected ConnFactory getConnFactory() {
        return new GreenplumFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.GREENPLUM6;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        Greenplum6SourceDTO greenplum6SourceDTO = (Greenplum6SourceDTO) iSource;

        // 校验 schema，特殊处理表名中带了 schema 信息的
        String tableName = queryDTO.getTableName();
        String schema = greenplum6SourceDTO.getSchema();
        if (StringUtils.isEmpty(greenplum6SourceDTO.getSchema())) {
            if (!queryDTO.getTableName().contains(".")) {
                throw new DtLoaderException("The greenplum data source requires schema parameters");
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
            throw new DtLoaderException(String.format("get table: %s's information error. Please contact the DBA to check the database、table information.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, DBUtil.clearAfterGetConnection(greenplum6SourceDTO, clearStatus));
        }
        return "";
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
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
            throw new DtLoaderException(String.format("get table: %s's information error. Please contact the DBA to check the database、table information.",
                    greenplum6SourceDTO.getSchema()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, DBUtil.clearAfterGetConnection(greenplum6SourceDTO, clearStatus));
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
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    protected String getCreateDatabaseSql(String dbName, String comment) {
        return String.format(CREATE_SCHEMA_SQL_TMPL, dbName);
    }

    /**
     * 此处方法为判断schema是否存在
     *
     * @param source 数据源信息
     * @param dbName schema 名称
     * @return 是否存在结果
     */
    @Override
    public Boolean isDatabaseExists(ISourceDTO source, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("schema  is not empty");
        }
        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(String.format(DATABASE_IS_EXISTS, dbName)).build()));
    }

    /**
     * 此处方法为判断指定schema 是否有该表
     *
     * @param source 数据源信息
     * @param tableName 表名
     * @param dbName schema名
     * @return 判断结果
     */
    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("schema  is not empty");
        }
        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(String.format(TABLES_IS_IN_SCHEMA, dbName, tableName)).build()));
    }

    @Override
    public String getShowDbSql() {
        return DATABASE_QUERY;
    }

    @Override
    protected String getCurrentDbSql() {
        return CURRENT_DB;
    }
}
