package com.dtstack.dtcenter.common.loader.kingbase;

import com.dtstack.dtcenter.common.loader.rdbms.AbsTableClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;


public class KingbaseTableClient extends AbsTableClient {

    private static final String ADD_COLUMN_SQL = "ALTER TABLE %s ADD COLUMN %s %s";

    private static final String ADD_COLUMN_COMMENT_SQL = "COMMENT ON COLUMN %s IS '%s'";

    @Override
    protected ConnFactory getConnFactory() {
        return new KingbaseConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.DB2;
    }

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public Boolean renameTable(ISourceDTO source, String oldTableName, String newTableName) {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public Boolean dropTable(ISourceDTO source, String tableName) {
        throw new DtLoaderException("The method is not supported");
    }


    /**
     * 添加表列名
     *
     * @param source
     * @param columnMetaDTO
     * @return
     */
    protected Boolean addTableColumn(ISourceDTO source, UpsertColumnMetaDTO columnMetaDTO) {
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        String schema = StringUtils.isNotBlank(columnMetaDTO.getSchema()) ? columnMetaDTO.getSchema() : rdbmsSourceDTO.getSchema();
        String tableName = transferSchemaAndTableName(schema, columnMetaDTO.getTableName());
        String sql = String.format(ADD_COLUMN_SQL, tableName, columnMetaDTO.getColumnName(), columnMetaDTO.getColumnType());
        executeSqlWithoutResultSet(source, sql);
        if (StringUtils.isNotEmpty(columnMetaDTO.getColumnComment())) {
            String commentSql = String.format(ADD_COLUMN_COMMENT_SQL, transformTableColumn(tableName, columnMetaDTO.getColumnName()), columnMetaDTO.getColumnComment());
            executeSqlWithoutResultSet(source, commentSql);
        }
        return true;
    }

}
