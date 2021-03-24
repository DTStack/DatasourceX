package com.dtstack.dtcenter.common.loader.rdbms;

import com.dtstack.dtcenter.common.loader.common.DtClassThreadFactory;
import com.dtstack.dtcenter.common.loader.common.exception.ErrorCode;
import com.dtstack.dtcenter.common.loader.common.utils.CollectionUtil;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.connection.CacheConnectionHelper;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:59 2020/1/3
 * @Description：客户端
 */
@Slf4j
public abstract class AbsRdbmsClient<T> implements IClient<T> {
    private ConnFactory connFactory = getConnFactory();

    /**
     * 获取连接工厂
     *
     * @return
     */
    protected abstract ConnFactory getConnFactory();

    /**
     * 获取数据源类型
     *
     * @return
     */
    protected abstract DataSourceType getSourceType();

    private static final String DONT_EXIST = "doesn't exist";

    private static final String SHOW_DB_SQL = "show databases";

    //线程池 - 用于部分数据源测试连通性超时处理
    protected static ExecutorService executor = new ThreadPoolExecutor(5, 10, 1L, TimeUnit.MINUTES, new ArrayBlockingQueue<>(5), new DtClassThreadFactory("testConnFactory"));

    /**
     * rdbms数据库获取连接唯一入口，对抛出异常进行统一处理
     * @param iSource
     * @return
     * @throws Exception
     */
    @Override
    public Connection getCon(ISourceDTO iSource) {
        log.info("-------getting connection....-----");
        if (!CacheConnectionHelper.isStart()) {
            try {
                return connFactory.getConn(iSource);
            } catch (Exception e){
                throw new DtLoaderException(e.getMessage(), e);
            }
        }

        return CacheConnectionHelper.getConnection(getSourceType().getVal(), con -> {
            try {
                return connFactory.getConn(iSource);
            } catch (Exception e) {
                throw new DtLoaderException(e.getMessage(), e);
            }
        });
    }

    @Override
    public Boolean testCon(ISourceDTO iSource) {
        return connFactory.testConn(iSource);
    }

    /**
     * 执行查询
     *
     * @param conn
     * @param queryDTO
     * @param clearStatus
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> executeQuery(Connection conn, SqlQueryDTO queryDTO, Integer clearStatus) throws SQLException {
        // 如果当前 connection 已关闭，直接返回空列表
        if (conn == null || conn.isClosed()) {
            return Lists.newArrayList();
        }

        // 预编译字段
        if (queryDTO.getPreFields() != null) {
            return DBUtil.executeQuery(conn, queryDTO.getSql(), ConnectionClearStatus.CLOSE.getValue().equals(clearStatus), queryDTO.getPreFields(), queryDTO.getQueryTimeout());
        }

        return DBUtil.executeQuery(conn, queryDTO.getSql(), queryDTO.getLimit(), queryDTO.getQueryTimeout(), ConnectionClearStatus.CLOSE.getValue().equals(clearStatus));
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(iSource, queryDTO, true);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) iSource;
        // 如果当前 connection 已关闭，直接返回空列表
        try {
            return executeQuery(rdbmsSourceDTO.clearAfterGetConnection(clearStatus), queryDTO, clearStatus);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("connection调用isClosed方法异常 : %s", e.getMessage()), e);
        }
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(iSource, queryDTO, true);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) iSource;
        try {
            // 如果当前 connection 已关闭，直接返回空列表
            if (rdbmsSourceDTO.getConnection().isClosed()) {
                return false;
            }
        } catch (SQLException e) {
            throw new DtLoaderException(String.format("connection调用isClosed方法异常 : %s", e.getMessage()), e);
        }

        DBUtil.executeSqlWithoutResultSet(rdbmsSourceDTO.clearAfterGetConnection(clearStatus), queryDTO.getSql(),
                ConnectionClearStatus.CLOSE.getValue().equals(clearStatus));
        return true;
    }

    /**
     * 执行查询前的操作
     *
     * @param iSource
     * @param queryDTO
     * @return 是否需要自动关闭连接
     * @throws Exception
     */
    protected Integer beforeQuery(ISourceDTO iSource, SqlQueryDTO queryDTO, boolean query) {
        // 查询 SQL 不能为空
        if (query && StringUtils.isBlank(queryDTO.getSql())) {
            throw new DtLoaderException("查询 SQL 不能为空");
        }

        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) iSource;
        // 设置 connection
        if (rdbmsSourceDTO.getConnection() == null) {
            rdbmsSourceDTO.setConnection(getCon(iSource));
            if (CacheConnectionHelper.isStart()) {
                return ConnectionClearStatus.NORMAL.getValue();
            }
            return ConnectionClearStatus.CLOSE.getValue();
        }
        return ConnectionClearStatus.NORMAL.getValue();
    }

    /**
     * 执行字段处理前的操作
     *
     * @param iSource
     * @param queryDTO
     * @return
     * @throws Exception
     */
    protected Integer beforeColumnQuery(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        // 查询表不能为空
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        if (queryDTO == null || StringUtils.isBlank(queryDTO.getTableName())) {
            throw new DtLoaderException("查询 表名称 不能为空");
        }

        queryDTO.setColumns(CollectionUtils.isEmpty(queryDTO.getColumns()) ? Collections.singletonList("*") :
                queryDTO.getColumns());
        return clearStatus;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) iSource;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            DatabaseMetaData meta = rdbmsSourceDTO.getConnection().getMetaData();
            if (null == queryDTO) {
                rs = meta.getTables(null, null, null, null);
            } else {
                rs = meta.getTables(null, rdbmsSourceDTO.getSchema(),
                        StringUtils.isBlank(queryDTO.getTableNamePattern()) ? queryDTO.getTableNamePattern() :
                                queryDTO.getTableName(),
                        DBUtil.getTableTypes(queryDTO));
            }
            while (rs.next()) {
                tableList.add(rs.getString(3));
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("获取数据库表异常：%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(rs, null, rdbmsSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

    /**
     * 暂不支持太多数据源
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        String sql = getTableBySchemaSql(source, queryDTO);
        return queryWithSingleColumn(source, sql, 1,"get table exception according to schema...");
    }

    /**
     * 执行查询sql，结果为单列
     *
     * @param source      数据源信息
     * @param sql         sql信息
     * @param errMsg      错误信息
     * @return 查询结果
     */
    protected List<String> queryWithSingleColumn(ISourceDTO source, String sql, Integer columnIndex, String errMsg) {
        Integer clearStatus = beforeQuery(source, SqlQueryDTO.builder().sql(sql).build(), true);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        log.info("The SQL executed by method queryWithSingleColumn is:{}", sql);
        Statement statement = null;
        ResultSet rs = null;
        List<String> result = new ArrayList<>();
        try {
            statement = rdbmsSourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                result.add(rs.getString(columnIndex == null ? 1 : columnIndex));
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("%s:%s", errMsg, e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, rdbmsSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return result;
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) iSource;

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = rdbmsSourceDTO.getConnection().createStatement();
            String queryColumnSql =
                    "select " + CollectionUtil.listToStr(queryDTO.getColumns()) + " from " + transferSchemaAndTableName(rdbmsSourceDTO.getSchema(), queryDTO.getTableName())
                            + " where 1=2";
            rs = stmt.executeQuery(queryColumnSql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int cnt = rsmd.getColumnCount();
            List<String> columnClassNameList = Lists.newArrayList();

            for (int i = 0; i < cnt; i++) {
                String columnClassName = rsmd.getColumnClassName(i + 1);
                columnClassNameList.add(columnClassName);
            }

            return columnClassNameList;
        } catch (Exception e){
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            DBUtil.closeDBResources(rs, stmt, rdbmsSourceDTO.clearAfterGetConnection(clearStatus));
        }
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(iSource, queryDTO, true);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) iSource;
        Statement statement = null;
        ResultSet rs = null;
        List<ColumnMetaDTO> columns = new ArrayList<>();
        try {
            statement = rdbmsSourceDTO.getConnection().createStatement();
            String queryColumnSql = queryDTO.getSql();
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(rsMetaData.getColumnLabel(i + 1));
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
                throw new DtLoaderException(queryDTO.getTableName() + "表不存在", e);
            } else {
                throw new DtLoaderException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息 ：%s",
                        queryDTO.getTableName(), e.getMessage()), e);
            }
        } finally {
            DBUtil.closeDBResources(rs, statement, rdbmsSourceDTO.clearAfterGetConnection(clearStatus));
        }
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) iSource;
        Statement statement = null;
        ResultSet rs = null;
        List<ColumnMetaDTO> columns = new ArrayList<>();
        try {
            statement = rdbmsSourceDTO.getConnection().createStatement();
            String queryColumnSql =
                    "select " + CollectionUtil.listToStr(queryDTO.getColumns()) + " from " + transferSchemaAndTableName(rdbmsSourceDTO.getSchema(), queryDTO.getTableName()) + " where 1=2";

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
        } catch (SQLException e) {
            if (e.getMessage().contains(DONT_EXIST)) {
                throw new DtLoaderException(queryDTO.getTableName() + "表不存在", e);
            } else {
                throw new DtLoaderException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息 ：%s",
                        queryDTO.getTableName(), e.getMessage()), e);
            }
        } finally {
            DBUtil.closeDBResources(rs, statement, rdbmsSourceDTO.clearAfterGetConnection(clearStatus));
        }

        //获取字段注释
        Map<String, String> columnComments = getColumnComments(rdbmsSourceDTO, queryDTO);
        if (Objects.isNull(columnComments)) {
            return columns;
        }
        for (ColumnMetaDTO columnMetaDTO : columns) {
            if (columnComments.containsKey(columnMetaDTO.getKey())) {
                columnMetaDTO.setComment(columnComments.get(columnMetaDTO.getKey()));
            }
        }
        return columns;

    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        return getColumnMetaData(iSource, queryDTO);
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        return "";
    }

    /**
     * rdbms数据预览
     * @param iSource
     * @param queryDTO
     * @return
     * @throws Exception
     */
    @Override
    public List<List<Object>> getPreview(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) iSource;
        List<List<Object>> previewList = new ArrayList<>();
        if (StringUtils.isBlank(queryDTO.getTableName())) {
            return previewList;
        }
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = rdbmsSourceDTO.getConnection().createStatement();
            //查询sql，默认预览100条
            String querySql = dealSql(rdbmsSourceDTO, queryDTO);
            if (queryDTO.getPreviewNum() != null) {
                stmt.setMaxRows(queryDTO.getPreviewNum());
            }
            rs = stmt.executeQuery(querySql);
            ResultSetMetaData rsmd = rs.getMetaData();
            //存储字段信息
            List<Object> metaDataList = Lists.newArrayList();
            //字段数量
            int len = rsmd.getColumnCount();
            for (int i = 0; i < len; i++) {
                metaDataList.add(rsmd.getColumnLabel(i + 1));
            }
            previewList.add(metaDataList);
            while (rs.next()){
                //一个columnData存储一行数据信息
                ArrayList<Object> columnData = Lists.newArrayList();
                for (int i = 0; i < len; i++) {
                    Object result = dealResult(rs.getObject(i + 1));
                    columnData.add(result);
                }
                previewList.add(columnData);
            }
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            DBUtil.closeDBResources(rs, stmt, rdbmsSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return previewList;
    }

    /**
     * 处理jdbc查询结果
     * @param result 查询结果
     * @return 处理后的结果
     */
    protected Object dealResult(Object result){
        return result;
    }

    /**
     * 处理sql语句预览条数
     * @param sqlQueryDTO 查询条件
     * @return 处理后的查询sql
     */
    protected String dealSql(ISourceDTO iSource, SqlQueryDTO sqlQueryDTO){
        return "select * from " + transferTableName(sqlQueryDTO.getTableName());
    }

    /**
     * 处理schema和表名
     *
     * @param schema
     * @param tableName
     * @return
     */
    protected String transferSchemaAndTableName(String schema,String tableName) {
        return transferTableName(tableName);
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

    protected Map<String, String> getColumnComments(RdbmsSourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        return null;
    }

    /**
     * 根据schema获取表，默认不支持。需要支持的数据源自己去实现该方法
     *
     * @param queryDTO 查询queryDTO
     * @return sql语句
     */
    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO){
        // 获取表信息需要通过show databases 语句
        String sql = getShowDbSql();
        return queryWithSingleColumn(source, sql, 1, "获取所有库异常");
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(source, queryDTO, false);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;

        // 获取表信息需要通过show databases 语句
        String tableName ;
        if (StringUtils.isNotEmpty(rdbmsSourceDTO.getSchema())) {
            tableName = rdbmsSourceDTO.getSchema() + "." + queryDTO.getTableName();
        } else {
            tableName = queryDTO.getTableName();
        }
        String sql = queryDTO.getSql()==null?"show create table "+tableName:queryDTO.getSql();
        Statement statement = null;
        ResultSet rs = null;
        String createTableSql =null;
        try {
            statement = rdbmsSourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                createTableSql = rs.getString(columnSize == 1 ? 1 : 2);
                break;
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("failed to get the create table sql：%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, rdbmsSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return createTableSql;
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        return Collections.emptyList();
    }

    @Override
    public Table getTable(ISourceDTO source, SqlQueryDTO queryDTO) {
        return new Table();
    }

    /**
     * 获取所有 数据库/schema sql语句
     * @return
     */
    protected String getShowDbSql(){
        return SHOW_DB_SQL;
    }

    @Override
    public String getCurrentDatabase(ISourceDTO source) {
        // 获取根据schema获取表的sql
        String sql = getCurrentDbSql();
        List<String> result = queryWithSingleColumn(source, sql, 1, "failed to get the currently used database");
        if (CollectionUtils.isEmpty(result)) {
            throw new DtLoaderException("failed to get the currently used database");
        }
        return result.get(0);
    }

    /**
     * 获取当前使用db的sql语句，需要支持的数据源中重写该方法
     * @return 对应的sql
     */
    protected String getCurrentDbSql() {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public Boolean createDatabase(ISourceDTO source, String dbName, String comment) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("database or schema cannot be empty");
        }
        String createSchemaSql = getCreateDatabaseSql(dbName, comment);
        return executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(createSchemaSql).build());
    }

    @Override
    public List<String> getCatalogs(ISourceDTO source) {
        String showCatalogsSql = getCatalogSql();
        return queryWithSingleColumn(source, showCatalogsSql, 1, "failed to get data source directory list");
    }

    /**
     * 获取创建库的sql，需要支持的数据源去实现该方法
     * @param dbName 库名
     * @param comment 注释
     * @return sql
     */
    protected String getCreateDatabaseSql(String dbName, String comment) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }


    @Override
    public Boolean isDatabaseExists(ISourceDTO source, String dbName) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    /**
     * 获取数据源/数据库目录列表的sql，需要实现的数据源去重写该方法
     * @return sql
     */
    protected String getCatalogSql() {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }
}
