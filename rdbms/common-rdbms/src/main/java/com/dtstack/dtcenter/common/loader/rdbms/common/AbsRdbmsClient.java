package com.dtstack.dtcenter.common.loader.rdbms.common;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.cache.connection.CacheConnectionHelper;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.*;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.CollectionUtil;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:59 2020/1/3
 * @Description：客户端
 */
public abstract class AbsRdbmsClient implements IClient {
    protected static final Logger logger = LoggerFactory.getLogger(ConnFactory.class);

    private ConnFactory connFactory = getConnFactory();

    protected abstract ConnFactory getConnFactory();

    protected abstract DataSourceType getSourceType();

    private static final String DONT_EXIST = "doesn't exist";

    @Override
    public Connection getCon(SourceDTO source) throws Exception {
        logger.info("-------get connection success-----");
        if (!CacheConnectionHelper.isStart()) {
            return connFactory.getConn(source);
        }

        return CacheConnectionHelper.getConnection(getSourceType().getVal(), con -> {
            try {
                return connFactory.getConn(source);
            } catch (Exception e) {
                throw new DtCenterDefException("获取连接异常", e);
            }
        });
    }

    @Override
    public Boolean testCon(SourceDTO source) {
        return connFactory.testConn(source);
    }

    @Override
    public List<Map<String, Object>> executeQuery(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeQuery(source, queryDTO, true);
        // 如果当前 connection 已关闭，直接返回空列表
        if (source.getConnection().isClosed()) {
            return Lists.newArrayList();
        }

        return DBUtil.executeQuery(source.getConnection(), queryDTO.getSql(), closeQuery);
    }

    @Override
    public Boolean executeSqlWithoutResultSet(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeQuery(source, queryDTO, true);
        // 如果当前 connection 已关闭，直接返回空列表
        if (source.getConnection().isClosed()) {
            return false;
        }

        DBUtil.executeSqlWithoutResultSet(source.getConnection(), queryDTO.getSql(),
                beforeQuery(source, queryDTO, true));
        return true;
    }

    /**
     * 执行查询前的操作
     *
     * @param sourceDTO
     * @param queryDTO
     * @return 是否需要自动关闭连接
     * @throws Exception
     */
    protected Boolean beforeQuery(SourceDTO sourceDTO, SqlQueryDTO queryDTO, boolean query) throws Exception {
        // 查询 SQL 不能为空
        if (query && StringUtils.isBlank(queryDTO.getSql())) {
            throw new DtLoaderException("查询 SQL 不能为空");
        }

        // 设置 connection
        boolean closeConn = false;
        if (sourceDTO.getConnection() == null) {
            sourceDTO.setConnection(getCon(sourceDTO));
            closeConn = true;
        }
        return closeConn;
    }

    /**
     * 执行字段处理前的操作
     *
     * @param sourceDTO
     * @param queryDTO
     * @return
     * @throws Exception
     */
    protected Boolean beforeColumnQuery(SourceDTO sourceDTO, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeQuery(sourceDTO, queryDTO, false);
        if (queryDTO == null || StringUtils.isBlank(queryDTO.getTableName())) {
            throw new DtLoaderException("查询 表名称 不能为空");
        }

        queryDTO.setColumns(CollectionUtils.isEmpty(queryDTO.getColumns()) ? Collections.singletonList("*") :
                queryDTO.getColumns());
        return closeQuery;
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeQuery(source, queryDTO, false);
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            DatabaseMetaData meta = source.getConnection().getMetaData();
            if (null == queryDTO) {
                rs = meta.getTables(null, null, null, null);
            } else {
                rs = meta.getTables(null,
                        StringUtils.isBlank(queryDTO.getSchemaPattern()) ? queryDTO.getSchemaPattern() :
                                queryDTO.getSchema(),
                        StringUtils.isBlank(queryDTO.getTableNamePattern()) ? queryDTO.getTableNamePattern() :
                                queryDTO.getTableName(),
                        DBUtil.getTableTypes(queryDTO));
            }
            while (rs.next()) {
                tableList.add(rs.getString(3));
            }
        } catch (Exception e) {
            throw new DtLoaderException("获取数据库表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, null, closeQuery ? source.getConnection() : null);
        }
        return tableList;
    }

    @Override
    public List<String> getColumnClassInfo(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeColumnQuery(source, queryDTO);

        Statement stmt = null;
        try {
            stmt = source.getConnection().createStatement();
            String queryColumnSql =
                    "select " + CollectionUtil.listToStr(queryDTO.getColumns()) + " from " + queryDTO.getTableName()
                            + " where 1=2";

            ResultSetMetaData rsmd = stmt.executeQuery(queryColumnSql).getMetaData();
            int cnt = rsmd.getColumnCount();
            List<String> columnClassNameList = Lists.newArrayList();

            for (int i = 0; i < cnt; i++) {
                String columnClassName = rsmd.getColumnClassName(i + 1);
                columnClassNameList.add(columnClassName);
            }

            return columnClassNameList;
        } finally {
            DBUtil.closeDBResources(null, stmt, closeQuery ? source.getConnection() : null);
        }
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeColumnQuery(source, queryDTO);
        Statement statement = null;
        ResultSet rs = null;

        List<ColumnMetaDTO> columns = new ArrayList<>();
        try {
            statement = source.getConnection().createStatement();
            String queryColumnSql =
                    "select " + CollectionUtil.listToStr(queryDTO.getColumns()) + " from " + transferTableName(queryDTO.getTableName()) + " where 1=2";

            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(rsMetaData.getColumnName(i + 1));
                columnMetaDTO.setType(doDealType(rsMetaData, i));
                columnMetaDTO.setPart(false);

                // 获取字段精度
                if (columnMetaDTO.getType().equalsIgnoreCase("decimal")
                        || columnMetaDTO.getType().equalsIgnoreCase("float")
                        || columnMetaDTO.getType().equalsIgnoreCase("double")
                        || columnMetaDTO.getType().equalsIgnoreCase("numeric")) {
                    columnMetaDTO.setScale(rsMetaData.getScale(i + 1));
                    columnMetaDTO.setPrecision(rsMetaData.getPrecision(i + 1));
                }

                columns.add(columnMetaDTO);
            }
            return columns;

        } catch (SQLException e) {
            if (e.getMessage().contains(DONT_EXIST)) {
                throw new DtCenterDefException(queryDTO.getTableName() + "表不存在", DBErrorCode.TABLE_NOT_EXISTS, e);
            } else {
                throw new DtCenterDefException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.",
                        queryDTO.getTableName()),
                        DBErrorCode.GET_COLUMN_INFO_FAILED, e);
            }
        } finally {
            DBUtil.closeDBResources(rs, statement, closeQuery ? source.getConnection() : null);
        }
    }

    @Override
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    /**
     * 处理表名
     *
     * @param tableName
     * @return
     */
    protected String transferTableName(String tableName) {
        return tableName;
    }

    /**
     * 处理字段类型
     */
    protected String doDealType(ResultSetMetaData rsMetaData, Integer los) throws SQLException {
        return rsMetaData.getColumnTypeName(los + 1);
    }

    /********************************* 关系型数据库无需实现的方法 ******************************************/
    @Override
    public String getAllBrokersAddress(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTopicList(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean createTopic(SourceDTO source, KafkaTopicDTO kafkaTopic) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<MetadataResponse.PartitionMetadata> getAllPartitions(SourceDTO source, String topic) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<KafkaOffsetDTO> getOffset(SourceDTO source, String topic) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
