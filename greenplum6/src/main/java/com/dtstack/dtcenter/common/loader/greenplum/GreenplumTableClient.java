package com.dtstack.dtcenter.common.loader.greenplum;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Greenplum表操作相关接口
 *
 * @author ：wangchuan
 * date：Created in 10:57 上午 2020/12/3
 * company: www.dtstack.com
 */
public class GreenplumTableClient implements ITable {

    private static final IClient GREENPLUM_CLIENT = ClientCache.getClient(DataSourceType.GREENPLUM6.getVal());

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) throws Exception {
        throw new DtLoaderException("greenplum不支持获取分区操作！");
    }

    @Override
    public Boolean dropTable(ISourceDTO source, String tableName) throws Exception {
        throw new DtLoaderException("greenplum暂时不支持删除表操作！");
    }

    @Override
    public Boolean renameTable(ISourceDTO source, String oldTableName, String newTableName) throws Exception {
        if (StringUtils.isBlank(oldTableName) || StringUtils.isBlank(newTableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        String renameTableSql = String.format("alter table %s rename to %s", oldTableName, newTableName);
        return GREENPLUM_CLIENT.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(renameTableSql).build());
    }

    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) throws Exception {
        throw new DtLoaderException("greenplum暂时不支持更改表参数操作！");
    }
}
