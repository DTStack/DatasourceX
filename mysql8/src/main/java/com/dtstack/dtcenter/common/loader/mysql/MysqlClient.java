package com.dtstack.dtcenter.common.loader.mysql;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.Mysql8SourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.DBUtil;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:00 2020/2/27
 * @Description：MySQL 客户端
 */
public class MysqlClient extends AbsRdbmsClient {

    private static final String DONT_EXIST = "doesn't exist";

    @Override
    protected ConnFactory getConnFactory() {
        return new MysqlConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.MySQL8;
    }

    @Override
    protected String transferTableName(String tableName) {
        return tableName.contains("`") ? tableName : String.format("`%s`", tableName);
    }

    @Override
    public String getTableMetaComment(ISourceDTO ISource, SqlQueryDTO queryDTO) throws Exception {
        Mysql8SourceDTO mysql8SourceDTO = (Mysql8SourceDTO) ISource;
        Integer clearStatus = beforeColumnQuery(mysql8SourceDTO, queryDTO);
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            statement = mysql8SourceDTO.getConnection().createStatement();
            resultSet = statement.executeQuery("show table status");
            while (resultSet.next()) {
                String dbTableName = resultSet.getString(1);

                if (dbTableName.equalsIgnoreCase(queryDTO.getTableName())) {
                    return resultSet.getString(DtClassConsistent.PublicConsistent.COMMENT);
                }
            }
        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, mysql8SourceDTO.clearAfterGetConnection(clearStatus));
        }
        return null;
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Mysql8SourceDTO mysql8SourceDTO = (Mysql8SourceDTO) source;
        MysqlDownloader mysqlDownloader = new MysqlDownloader(getCon(source), queryDTO.getSql(), mysql8SourceDTO.getSchema());
        mysqlDownloader.configure();
        return mysqlDownloader;
    }

    @Override
    protected Map<String, String> getColumnComments(RdbmsSourceDTO sourceDTO, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(sourceDTO, queryDTO);
        Statement statement = null;
        ResultSet rs = null;
        Map<String, String> columnComments = new HashMap<>();
        try {
            statement = sourceDTO.getConnection().createStatement();
            String queryColumnCommentSql =
                    "show full columns from " + transferTableName(queryDTO.getTableName());
            rs = statement.executeQuery(queryColumnCommentSql);
            while (rs.next()) {
                String columnName = rs.getString("Field");
                String columnComment = rs.getString("Comment");
                columnComments.put(columnName, columnComment);
            }

        } catch (Exception e) {
            if (e.getMessage().contains(DONT_EXIST)) {
                throw new DtCenterDefException(queryDTO.getTableName() + "表不存在", DBErrorCode.TABLE_NOT_EXISTS, e);
            } else {
                throw new DtCenterDefException(String.format("获取表:%s 的字段的注释信息时失败. 请联系 DBA 核查该库、表信息.",
                        queryDTO.getTableName()),
                        DBErrorCode.GET_COLUMN_INFO_FAILED, e);
            }
        }finally {
            DBUtil.closeDBResources(rs, statement, sourceDTO.clearAfterGetConnection(clearStatus));
        }
        return columnComments;
    }

}
