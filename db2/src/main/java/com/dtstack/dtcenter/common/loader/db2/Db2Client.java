package com.dtstack.dtcenter.common.loader.db2;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Db2SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.common.loader.common.DBUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:09 2020/1/7
 * @Description：Db2 客户端
 */
public class Db2Client extends AbsRdbmsClient {
    private static final String TABLE_QUERY = "select tabname from syscat.tables where tabschema = '%s'";

    private static final String DATABASE_QUERY = "select schemaname from syscat.schemata where ownertype != 'S'";

    @Override
    protected ConnFactory getConnFactory() {
        return new Db2ConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.DB2;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        Db2SourceDTO db2SourceDTO = (Db2SourceDTO) iSource;
        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            String sql = String.format(TABLE_QUERY, db2SourceDTO.getUsername().toUpperCase());
            statement = db2SourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new DtLoaderException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, db2SourceDTO.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        Db2SourceDTO db2SourceDTO = (Db2SourceDTO) iSource;

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = db2SourceDTO.getConnection().createStatement();
            resultSet = statement.executeQuery(String.format("select remarks from syscat.tables where tabname = '%s'"
                    , queryDTO.getTableName()));
            while (resultSet.next()) {
                return resultSet.getString("REMARKS");
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, db2SourceDTO.clearAfterGetConnection(clearStatus));
        }
        return "";
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Db2SourceDTO db2SourceDTO = (Db2SourceDTO) source;
        Connection connection = getCon(source);
        String sql = queryDTO.getSql();
        String schema = db2SourceDTO.getSchema();
        Db2Downloader db2Downloader = new Db2Downloader(connection, sql, schema);
        db2Downloader.configure();
        return db2Downloader;
    }

    @Override
    public String getShowDbSql() {
        return DATABASE_QUERY;
    }
}
