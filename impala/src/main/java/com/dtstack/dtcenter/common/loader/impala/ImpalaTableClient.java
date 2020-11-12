package com.dtstack.dtcenter.common.loader.impala;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * impala表操作相关接口
 *
 * @author ：wangchuan
 * date：Created in 10:57 上午 2020/12/3
 * company: www.dtstack.com
 */
@Slf4j
public class ImpalaTableClient implements ITable {

    private static final IClient IMPALA_Client = ClientCache.getClient(DataSourceType.IMPALA.getVal());

    private static final String SHOW_PARTITIONS_SQL = "show partitions %s";

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) throws Exception {
        log.info("impala获取表分区，表名：{}", tableName);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("查询表不能为空！");
        }
        Connection con = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            con = IMPALA_Client.getCon(source);
            Set<String> res = new HashSet<>();
            try {
                statement = con.createStatement();
                resultSet = statement.executeQuery(String.format(SHOW_PARTITIONS_SQL, tableName));
            } catch (SQLException e) {
                //表没有创建分区异常 500051
                //其他异常直接抛出
                if (e.getErrorCode() != 500051) {
                    throw e;
                }
            }
            if (resultSet == null) {
                return Lists.newArrayList();
            }
            //如果存在分区 判断分区类型是一般分区 还是基于kudu的分区，kudu分区第一列为#Rows
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCnt = resultSetMetaData.getColumnCount();
            List<String> columnList = new ArrayList<>();
            for (int i = 0; i < columnCnt; i++) {
                String column = resultSetMetaData.getColumnName(i + 1).replaceAll(" ", "");
                if (column.matches("#Rows")) {
                    break;
                }
                columnList.add(column);
            }
            //格式化分区信息 与hive保持一致
            StringJoiner tempJoiner = new StringJoiner("=/", "", "=");
            for (String s : columnList) {
                tempJoiner.add(s);
            }
            res.add(tempJoiner.toString());
            return new ArrayList<>(res);
        } catch (Exception e) {
            log.error("获取分区信息失败", e);
            throw new DtLoaderException("获取分区信息失败", e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, con);
        }
    }


    @Override
    public Boolean dropTable(ISourceDTO source, String tableName) throws Exception {
        log.info("impala删除表，表名：{}", tableName);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("删除表不能为空！");
        }
        String dropTableSql = String.format("drop table if exists `%s`", tableName);
        return IMPALA_Client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(dropTableSql).build());
    }

    @Override
    public Boolean renameTable(ISourceDTO source, String oldTableName, String newTableName) throws Exception {
        log.info("impala重命名表，旧表名：{}，新表名：{}", oldTableName, newTableName);
        if (StringUtils.isBlank(oldTableName) || StringUtils.isBlank(newTableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        String renameTableSql = String.format("alter table %s rename to %s", oldTableName, newTableName);
        return IMPALA_Client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(renameTableSql).build());
    }

    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) throws Exception {
        log.info("impala更改表参数，表名：{}，参数：{}", tableName, params);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        if (params == null || params.isEmpty()) {
            throw new DtLoaderException("表参数不能为空！");
        }
        List<String> tableProperties = Lists.newArrayList();
        params.forEach((key, val) -> tableProperties.add(String.format("'%s'='%s'", key, val)));
        String alterTableParamsSql = String.format("alter table %s set tblproperties (%s)", tableName, StringUtils.join(tableProperties, "."));
        return IMPALA_Client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(alterTableParamsSql).build());
    }
}
