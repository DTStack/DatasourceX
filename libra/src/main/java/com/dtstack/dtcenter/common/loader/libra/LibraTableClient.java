package com.dtstack.dtcenter.common.loader.libra;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Libra表操作相关接口
 *
 * @author ：wangchuan
 * date：Created in 10:57 上午 2020/12/3
 * company: www.dtstack.com
 */
@Slf4j
public class LibraTableClient implements ITable {

    private static final IClient LIBRA_CLIENT = ClientCache.getClient(DataSourceType.LIBRA.getVal());

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) throws Exception {
        throw new DtLoaderException("Libra不支持获取分区操作！");
    }

    @Override
    public Boolean dropTable(ISourceDTO source, String tableName) throws Exception {
        log.info("libra删除表，表名：{}", tableName);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("要删除的表名不能为空！");
        }
        String dropTableSql = String.format("drop table if exists %s", tableName);
        return LIBRA_CLIENT.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(dropTableSql).build());
    }

    @Override
    public Boolean renameTable(ISourceDTO source, String oldTableName, String newTableName) throws Exception {
        log.info("libra重命名表，原表名：{}, 新表名：{}", oldTableName, newTableName);
        if (StringUtils.isBlank(oldTableName) || StringUtils.isBlank(newTableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        String renameTableSql = String.format("alter table %s rename to %s", oldTableName, newTableName);
        return LIBRA_CLIENT.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(renameTableSql).build());
    }

    /**
     * 更改表相关参数，暂时只支持更改表注释
     * @param source 数据源信息
     * @param tableName 表名
     * @param params 修改的参数，map集合
     * @return 执行结果
     */
    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) throws Exception {
        String comment = params.get("comment");
        log.info("libra更改表注释，comment：{}！", comment);
        if (StringUtils.isEmpty(comment)) {
            return true;
        }
        String alterTableParamsSql = String.format("COMMENT ON TABLE %s IS '%s'", tableName, comment);
        return LIBRA_CLIENT.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(alterTableParamsSql).build());
    }
}
