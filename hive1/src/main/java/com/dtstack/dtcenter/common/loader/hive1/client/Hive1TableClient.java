package com.dtstack.dtcenter.common.loader.hive1.client;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * hive1表操作相关接口
 *
 * @author ：wangchuan
 * date：Created in 10:57 上午 2020/12/3
 * company: www.dtstack.com
 */
@Slf4j
public class Hive1TableClient implements ITable {

    private static final IClient hive1Client = ClientCache.getClient(DataSourceType.HIVE1X.getVal());

    private static final String SHOW_PARTITIONS_SQL = "show partitions %s";

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) throws Exception {
        log.info("hive1.x获取表分区，表名：{}", tableName);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("查询表不能为空！");
        }
        List<Map<String, Object>> result = hive1Client.executeQuery(source, SqlQueryDTO.builder().sql(String.format(SHOW_PARTITIONS_SQL, tableName)).build());
        List<String> partitions = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(result)) {
            result.forEach(rs -> partitions.add(MapUtils.getString(rs, "partition")));
        }
        return partitions;
    }

    @Override
    public Boolean dropTable(ISourceDTO source, String tableName) throws Exception {
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("删除表不能为空！");
        }
        String dropTableSql = String.format("drop table if exists `%s`", tableName);
        return hive1Client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(dropTableSql).build());
    }

    @Override
    public Boolean renameTable(ISourceDTO source, String oldTableName, String newTableName) throws Exception {
        log.info("hive1.x重命名表，旧表名：{}，新表名：{}", oldTableName, newTableName);
        if (StringUtils.isBlank(oldTableName) || StringUtils.isBlank(newTableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        String renameTableSql = String.format("alter table %s rename to %s", oldTableName, newTableName);
        return hive1Client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(renameTableSql).build());
    }

    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) throws Exception {
        log.info("hive1.x更改表参数，表名：{}，参数：{}", tableName, params);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        if (params == null || params.isEmpty()) {
            throw new DtLoaderException("表参数不能为空！");
        }
        List<String> tableProperties = Lists.newArrayList();
        params.forEach((key, val) -> tableProperties.add(String.format("'%s'='%s'", key, val)));
        String alterTableParamsSql = String.format("alter table %s set tblproperties (%s)", tableName, StringUtils.join(tableProperties, "."));
        return hive1Client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(alterTableParamsSql).build());
    }
}
