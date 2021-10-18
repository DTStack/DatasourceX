package com.dtstack.dtcenter.common.loader.hive1.client;

import com.dtstack.dtcenter.common.loader.hive1.HiveConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.AbsTableClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
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
public class Hive1TableClient extends AbsTableClient {

    @Override
    protected ConnFactory getConnFactory() {
        return new HiveConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.HIVE1X;
    }

    private static final String TABLE_IS_VIEW_SQL = "desc formatted %s";

    private static final String ADD_COLUMN_SQL = "alter table %s add columns(%s %s comment '%s')";

    @Override
    public Boolean isView(ISourceDTO source, String schema, String tableName) {
        checkParamAndSetSchema(source, schema, tableName);
        String sql = String.format(TABLE_IS_VIEW_SQL, tableName);
        List<Map<String, Object>> result = executeQuery(source, sql);
        if (CollectionUtils.isEmpty(result)) {
            throw new DtLoaderException(String.format("Execute to determine whether the table is a view sql result is empty，sql：%s", sql));
        }
        String tableType = "";
        for (Map<String, Object> row : result) {
            String colName = MapUtils.getString(row, "col_name");
            if (StringUtils.containsIgnoreCase(colName, "Table Type")) {
                tableType = MapUtils.getString(row, "data_type");
                break;
            }
        }
        if (StringUtils.isEmpty(tableType)) {
            for (Map<String, Object> row : result) {
                String colName = MapUtils.getString(row, "col_name");
                if (StringUtils.containsIgnoreCase(colName, "Type")) {
                    tableType = MapUtils.getString(row, "data_type");
                    break;
                }
            }
        }
        log.info("table schema :{},table name:{}, type:{}", schema, tableName, tableType);
        return StringUtils.containsIgnoreCase(tableType, "VIEW");
    }

    /**
     * 添加表字段
     *
     * @param source
     * @param columnMetaDTO
     * @return
     */
    protected Boolean addTableColumn(ISourceDTO source, UpsertColumnMetaDTO columnMetaDTO) {
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        String schema = StringUtils.isNotBlank(columnMetaDTO.getSchema()) ? columnMetaDTO.getSchema() : rdbmsSourceDTO.getSchema();
        String comment = StringUtils.isNotBlank(columnMetaDTO.getColumnComment()) ? columnMetaDTO.getColumnComment() : "";
        String sql = String.format(ADD_COLUMN_SQL, transferSchemaAndTableName(schema, columnMetaDTO.getTableName()), columnMetaDTO.getColumnName(), columnMetaDTO.getColumnType(), comment);
        return executeSqlWithoutResultSet(source, sql);
    }
}
