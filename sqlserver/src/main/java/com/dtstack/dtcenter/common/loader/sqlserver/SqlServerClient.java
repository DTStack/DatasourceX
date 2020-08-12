package com.dtstack.dtcenter.common.loader.sqlserver;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.downloader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SqlserverSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.utils.DBUtil;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:30 2020/1/7
 * @Description：SqlServer 客户端
 */
public class SqlServerClient extends AbsRdbmsClient {
    private static final String TABLE_QUERY_ALL = "select * from sys.objects where type='U' or type='V'";
    private static final String TABLE_QUERY = "select * from sys.objects where type='U'";
    //获取所有的表和对应的schema-备用
    private static final String TABLE_QUERY_ALL_SCHEMA = "select sys.objects.name tableName,sys.schemas.name schemaName from sys.objects,sys.schemas where sys.objects.type='U' and sys.objects.schema_id=sys.schemas.schema_id";
    private static final String TABLE_QUERY_SCHEMA = "select sys.objects.name tableName,sys.schemas.name schemaName from sys.objects,sys.schemas where sys.objects.type='U' or type='V' and sys.objects.schema_id=sys.schemas.schema_id";

    @Override
    protected ConnFactory getConnFactory() {
        return new SQLServerConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.SQLServer;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        SqlserverSourceDTO sqlserverSourceDTO = (SqlserverSourceDTO) iSource;
        Integer clearStatus = beforeQuery(sqlserverSourceDTO, queryDTO, false);

        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            String sql = queryDTO.getView() ? TABLE_QUERY_ALL : TABLE_QUERY;
            statement = sqlserverSourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, sqlserverSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        SqlserverSourceDTO sqlserverSourceDTO = (SqlserverSourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(sqlserverSourceDTO, queryDTO);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = sqlserverSourceDTO.getConnection().createStatement();
            resultSet = statement.executeQuery(
                    "select c.name, cast(isnull(f.[value], '') as nvarchar(100)) as REMARKS\n" +
                    "from sys.objects c " +
                    "left join sys.extended_properties f on f.major_id = c.object_id and f.minor_id = 0 and f.class = 1\n" +
                    "where c.type = 'u'");
            while (resultSet.next()) {
                String dbTableName = resultSet.getString(1);
                if (dbTableName.equalsIgnoreCase(queryDTO.getTableName())) {
                    return resultSet.getString(DtClassConsistent.PublicConsistent.REMARKS);
                }
            }
        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, sqlserverSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return null;
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        SqlserverSourceDTO sqlserverSourceDTO = (SqlserverSourceDTO) source;
        SqlServerDownloader sqlServerDownloader = new SqlServerDownloader(getCon(sqlserverSourceDTO), queryDTO.getSql(), sqlserverSourceDTO.getSchema());
        sqlServerDownloader.configure();
        return sqlServerDownloader;
    }

    @Override
    protected String dealSql(SqlQueryDTO sqlQueryDTO) {
        return "select top "+sqlQueryDTO.getPreviewNum()+" * from "+transferTableName(sqlQueryDTO.getTableName());
    }

    @Override
    protected String transferTableName(String tableName) {
        //如果传过来是[tableName]格式直接当成表名
        if (tableName.startsWith("[") && tableName.endsWith("]")){
            return tableName;
        }
        //如果不是上述格式，判断有没有"."符号，有的话，第一个"."之前的当成schema，后面的当成表名进行[tableName]处理
        if (tableName.contains(".")) {
            //切割，表名中可能会有包含"."的情况，所以限制切割后长度为2
            String[] tables = tableName.split("\\.", 2);
            tableName = tables[1];
            return String.format("%s.%s", tables[0], tableName.contains("[") ? tableName : String.format("[%s]",
                    tableName));
        }
        //判断表名
        return String.format("[%s]", tableName);
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
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
