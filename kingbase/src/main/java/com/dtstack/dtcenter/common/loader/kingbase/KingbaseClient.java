package com.dtstack.dtcenter.common.loader.kingbase;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KingbaseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * company: www.dtstack.com
 * @author ：忘川
 * Date ：Created in 17:18 2020/09/01
 * Description：kingbase 客户端
 */
public class KingbaseClient extends AbsRdbmsClient {

    //获取所有schema，去除系统库
    private static final String SCHEMA_SQL = "SELECT NSPNAME FROM SYS_CATALOG.SYS_NAMESPACE WHERE NSPNAME !~ 'sys' AND NSPNAME <> 'information_schema' ORDER BY NSPNAME ";

    //获取某个schema下的所有表
    private static final String SCHEMA_TABLE_SQL = "SELECT tablename FROM SYS_CATALOG.sys_tables WHERE schemaname = '%s' ";

    //获取用户下的表
    private static final String USER_TABLE_SQL = "SELECT TABLE_NAME FROM user_tables ";

    //获取某个表的表注释信息
    private static final String TABLE_COMMENT_SQL = "SELECT COMMENTS FROM ALL_TAB_COMMENTS WHERE TABLE_NAME = '%s' ";

    //获取某个表的字段注释信息
    private static final String COL_COMMENT_SQL = "SELECT COLUMN_NAME,COMMENTS FROM ALL_COL_COMMENTS WHERE TABLE_NAME = '%s' ";

    @Override
    protected ConnFactory getConnFactory() {
        return new KingbaseConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.KINGBASE;
    }

    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        KingbaseSourceDTO kingbaseSourceDTO = (KingbaseSourceDTO) source;
        Integer clearStatus = beforeQuery(kingbaseSourceDTO, queryDTO, false);
        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = kingbaseSourceDTO.getConnection().createStatement();
            //不区分大小写
            rs = statement.executeQuery(StringUtils.isNotBlank(kingbaseSourceDTO.getSchema()) ?
                    String.format(SCHEMA_TABLE_SQL, kingbaseSourceDTO.getSchema()) : USER_TABLE_SQL);
            List<String> tableList = new ArrayList<>();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
            return tableList;
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, kingbaseSourceDTO.clearAfterGetConnection(clearStatus));
        }
    }

    /**
     * 获取表注释信息
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    @Override
    public String getTableMetaComment(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        KingbaseSourceDTO kingbaseSourceDTO = (KingbaseSourceDTO) source;
        Integer clearStatus = beforeColumnQuery(kingbaseSourceDTO, queryDTO);
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            statement = kingbaseSourceDTO.getConnection().createStatement();
            resultSet = statement.executeQuery(TABLE_COMMENT_SQL);
            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, kingbaseSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return "";
    }

    /**
     * 处理kingbase schema和tableName，适配schema和tableName中有.的情况
     * @param schema
     * @param tableName
     * @return
     */
    @Override
    protected String transferSchemaAndTableName(String schema, String tableName) {
        if (!tableName.startsWith("\"") || !tableName.endsWith("\"")) {
            tableName = String.format("\"%s\"", tableName);
        }
        if (StringUtils.isBlank(schema)) {
            return tableName;
        }
        if (!schema.startsWith("\"") || !schema.endsWith("\"")){
            schema = String.format("\"%s\"", schema);
        }
        return String.format("%s.%s", schema, tableName);
    }

    /**
     * 获取所有 数据库/schema sql语句
     * @return
     */
    @Override
    protected String getShowDbSql(){
        return SCHEMA_SQL;
    }

    /**
     * 获取字段注释
     * @param sourceDTO
     * @param queryDTO
     * @return
     * @throws Exception
     */
    @Override
    protected Map<String, String> getColumnComments(RdbmsSourceDTO sourceDTO, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(sourceDTO, queryDTO);
        Statement statement = null;
        ResultSet rs = null;
        Map<String, String> columnComments = new HashMap<>();
        try {
            statement = sourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(COL_COMMENT_SQL);
            while (rs.next()) {
                String columnName = rs.getString("TABLE_NAME");
                String columnComment = rs.getString("COMMENTS");
                columnComments.put(columnName, columnComment);
            }

        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的字段的注释信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        }finally {
            DBUtil.closeDBResources(rs, statement, sourceDTO.clearAfterGetConnection(clearStatus));
        }
        return columnComments;
    }

    /**
     * 处理kingbase数据预览sql
     * @param rdbmsSourceDTO
     * @param sqlQueryDTO
     * @return
     */
    @Override
    protected String dealSql(RdbmsSourceDTO rdbmsSourceDTO, SqlQueryDTO sqlQueryDTO){
        return "select * from " + transferSchemaAndTableName(rdbmsSourceDTO.getSchema(), sqlQueryDTO.getTableName())
                + " limit " + sqlQueryDTO.getPreviewNum();
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

}
