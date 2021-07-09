package com.dtstack.dtcenter.common.loader.doris;

import com.dtstack.dtcenter.common.loader.mysql5.MysqlClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.lang3.StringUtils;

/**
 * @author ：qianyi
 * date：Created in 下午1:46 2021/07/09
 * company: www.dtstack.com
 */
public class DorisClient extends MysqlClient {
    /**
     * 查询的格式: default_cluster:dbName
     *
     * @param source
     * @return
     */
    @Override
    public String getCurrentDatabase(ISourceDTO source) {
        String currentDb = super.getCurrentDatabase(source);
        return currentDb.substring(currentDb.lastIndexOf(":") + 1);
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new DorisConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.DORIS;
    }


    @Override
    protected String transferSchemaAndTableName(String schema, String tableName) {
        if (StringUtils.isBlank(schema)) {
            return tableName;
        }
        return String.format("%s.%s", schema, tableName);
    }
}
