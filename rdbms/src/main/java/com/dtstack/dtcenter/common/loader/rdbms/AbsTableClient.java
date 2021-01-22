package com.dtstack.dtcenter.common.loader.rdbms;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.common.utils.MathUtil;
import com.dtstack.dtcenter.loader.cache.connection.CacheConnectionHelper;
import com.dtstack.dtcenter.loader.client.ITable;
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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * <p>表操作相关抽象客户端</>
 *
 * @author ：wangchuan
 * date：Created in 2:08 下午 2020/12/12
 * company: www.dtstack.com
 */
@Slf4j
public abstract class AbsTableClient implements ITable {

    // 连接工厂
    private final ConnFactory connFactory = getConnFactory();

    // 数据源类型
    private final DataSourceType dataSourceType = getSourceType();

    // 获取所有分区
    protected static final String SHOW_PARTITIONS_SQL = "show partitions %s";

    /**
     * 获取连接工厂
     *
     * @return 连接工程
     */
    protected abstract ConnFactory getConnFactory();

    /**
     * 获取数据源类型
     *
     * @return 数据源类型
     */
    protected abstract DataSourceType getSourceType();

    /**
     * rdbms数据库获取连接唯一入口，对抛出异常进行统一处理
     * @param sourceDTO 数据源信息
     * @return 连接
     * @throws Exception 异常
     */
    @Override
    public Connection getCon(ISourceDTO sourceDTO) {
        log.info("-------getting connection....-----");
        if (!CacheConnectionHelper.isStart()) {
            try {
                return connFactory.getConn(sourceDTO);
            } catch (DtLoaderException e) {
                // 定义过的dtLoaderException直接抛出
                throw e;
            } catch (Exception e){
                throw new DtLoaderException("获取数据库连接异常！", e);
            }
        }

        return CacheConnectionHelper.getConnection(dataSourceType.getVal(), con -> {
            try {
                return connFactory.getConn(sourceDTO);
            } catch (DtLoaderException e) {
                // 定义过的dtLoaderException直接抛出
                throw e;
            } catch (Exception e) {
                throw new DtLoaderException("获取数据库连接异常", e);
            }
        });
    }

    /**
     * 执行sql查询
     *
     * @param sourceDTO 数据源信息
     * @param sql 查询sql
     * @return 查询结果
     * @throws Exception 异常
     */
    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO sourceDTO, String sql) {
        Integer clearStatus = beforeQuery(sourceDTO, sql, true);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) sourceDTO;
        // 如果当前 connection 已关闭，直接返回空列表
        try {
            if (rdbmsSourceDTO.getConnection().isClosed()) {
                return Lists.newArrayList();
            }
        } catch (SQLException e) {
            throw new DtLoaderException(String.format("检测连接是否关闭时异常:%s", e.getMessage()), e);
        }
        return DBUtil.executeQuery(rdbmsSourceDTO.clearAfterGetConnection(clearStatus), sql,
                ConnectionClearStatus.CLOSE.getValue().equals(clearStatus));
    }

    /**
     * 执行sql，不需要结果
     *
     * @param sourceDTO 数据源信息
     * @param sql 查询sql
     * @return 执行成功与否
     * @throws Exception 异常
     */
    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO sourceDTO, String sql) {
        Integer clearStatus = beforeQuery(sourceDTO, sql, true);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) sourceDTO;
        // 如果当前 connection 已关闭，直接返回空列表
        try {
            if (rdbmsSourceDTO.getConnection().isClosed()) {
                return false;
            }
        } catch (SQLException e) {
            throw new DtLoaderException(String.format("检测连接是否关闭时异常:%s", e.getMessage()), e);
        }
        DBUtil.executeSqlWithoutResultSet(rdbmsSourceDTO.clearAfterGetConnection(clearStatus), sql,
                ConnectionClearStatus.CLOSE.getValue().equals(clearStatus));
        return true;
    }

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) {
        log.info("获取表所有分区，表名：{}", tableName);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        List<Map<String, Object>> result = executeQuery(source, String.format(SHOW_PARTITIONS_SQL, tableName));
        List<String> partitions = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(result)) {
            result.forEach(rs -> partitions.add(MapUtils.getString(rs, "partition")));
        }
        return partitions;
    }

    @Override
    public Boolean dropTable(ISourceDTO source, String tableName) {
        log.info("删除表，表名：{}", tableName);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        String dropTableSql = getDropTableSql(tableName);
        return executeSqlWithoutResultSet(source, dropTableSql);
    }

    /**
     * 获取删除表的sql
     * @param tableName 表名
     * @return sql
     */
    protected String getDropTableSql(String tableName){
        return String.format("drop table if exists `%s`", tableName);
    };

    @Override
    public Boolean renameTable(ISourceDTO source, String oldTableName, String newTableName) {
        log.info("重命名表，旧表名：{}，新表名：{}", oldTableName, newTableName);
        if (StringUtils.isBlank(oldTableName) || StringUtils.isBlank(newTableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        String renameTableSql = String.format("alter table %s rename to %s", oldTableName, newTableName);
        return executeSqlWithoutResultSet(source, renameTableSql);
    }

    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) {
        log.info("更改表参数，表名：{}，参数：{}", tableName, params);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        if (params == null || params.isEmpty()) {
            throw new DtLoaderException("表参数不能为空！");
        }
        List<String> tableProperties = Lists.newArrayList();
        params.forEach((key, val) -> tableProperties.add(String.format("'%s'='%s'", key, val)));
        String alterTableParamsSql = String.format("alter table %s set tblproperties (%s)", tableName, StringUtils.join(tableProperties, "."));
        return executeSqlWithoutResultSet(source, alterTableParamsSql);
    }

    @Override
    public Long getTableSize(ISourceDTO source, String schema, String tableName) {
        log.info("获取表占用存储，schema：{}，表名：{}", schema, tableName);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        String tableSizeSql = getTableSizeSql(schema, tableName);
        log.info("获取占用存储的sql：{}", tableSizeSql);
        List<Map<String, Object>> result = executeQuery(source, tableSizeSql);
        if (CollectionUtils.isEmpty(result) || MapUtils.isEmpty(result.get(0))) {
            throw new DtLoaderException("获取表占用存储信息异常");
        }
        Object tableSize = result.get(0).values().stream().findFirst().orElseThrow(() -> new DtLoaderException("获取表占用存储信息异常"));
        return MathUtil.getLongVal(tableSize);
    }

    @Override
    public Boolean isView(ISourceDTO source, String schema, String tableName) {
        throw new DtLoaderException("该数据源暂时不支持该方法");
    }

    /**
     * 检查参数并设置schema
     * @param source 数据源信息
     * @param schema  schema名称
     * @param tableName 表名
     */
    protected void checkParamAndSetSchema (ISourceDTO source, String schema, String tableName) {
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("表名不能为空");
        }
        if (StringUtils.isNotBlank(schema)) {
            RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
            rdbmsSourceDTO.setSchema(schema);
        }
    }

    /**
     * 获取表占用存储的sql
     * @param schema schema信息
     * @param tableName 表名
     * @return 占用存储sql
     */
    protected String getTableSizeSql(String schema, String tableName) {
       throw new DtLoaderException("该数据源暂时不支持获取表占用存储");
    }

    /**
     * 执行查询前的操作
     *
     * @param sourceDTO 数据源信息
     * @param sql 执行sql
     * @param query 是否是查询操作
     * @return 是否需要自动关闭连接
     */
    protected Integer beforeQuery(ISourceDTO sourceDTO, String sql, boolean query) {
        // 如果是查询操作查询 SQL 不能为空
        if (query && StringUtils.isBlank(sql)) {
            throw new DtLoaderException("查询 SQL 不能为空");
        }
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) sourceDTO;
        // 设置 connection
        if (rdbmsSourceDTO.getConnection() == null) {
            rdbmsSourceDTO.setConnection(getCon(sourceDTO));
            if (CacheConnectionHelper.isStart()) {
                return ConnectionClearStatus.NORMAL.getValue();
            }
            return ConnectionClearStatus.CLOSE.getValue();
        }
        return ConnectionClearStatus.NORMAL.getValue();
    }
}
