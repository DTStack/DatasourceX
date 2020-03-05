package com.dtstack.dtcenter.common.loader.rdbms.kudu;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:00 2020/2/27
 * @Description：Kudu 客户端
 */
public class KuduClient extends AbsRdbmsClient {

    public static final int TIME_OUT = 5 * 1000;

    @Override
    public Boolean testCon(SourceDTO source) {
        if (null == source || StringUtils.isBlank(source.getUrl())) {
            return false;
        }

        boolean check = false;

        org.apache.kudu.client.KuduClient client = null;
        try {
            client = getConnection(source);
            try {
                client.getTablesList();
                check = true;
            } catch (KuduException e) {
                logger.error(e.getMessage(), e);
            }
        } finally {
            if (null != client) {
                try {
                    client.close();
                } catch (KuduException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        return check;
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        List<String> tableList = null;
        try {
            tableList = getConnection(source).getTablesList().getTablesList();
        } catch (KuduException e) {
            logger.error(e.getMessage(), e);
        }
        return tableList;
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        if (queryDTO == null || StringUtils.isBlank(queryDTO.getTableName())) {
            throw new DtCenterDefException("表名称不能为空");
        }

        org.apache.kudu.client.KuduClient client = null;
        try {
            client = getConnection(source);
            return getTableColumns(client, queryDTO.getTableName());
        } catch (DtCenterDefException e) {
            throw new DtCenterDefException(e.getMessage(), e);
        } finally {
            client.close();
        }
    }

    private List<ColumnMetaDTO> getTableColumns(org.apache.kudu.client.KuduClient client, String tableName) {
        if (StringUtils.isBlank(tableName)) {
            return null;
        }

        List<ColumnMetaDTO> metaDTOS = new ArrayList<>();
        try {
            KuduTable kuduTable = client.openTable(tableName);
            Schema schema = kuduTable == null ? null : kuduTable.getSchema();
            List<ColumnSchema> columnSchemas = kuduTable == null ? null : schema.getColumns();
            if (CollectionUtils.isEmpty(columnSchemas)) {
                return Collections.emptyList();
            }

            columnSchemas.stream().forEach(record -> {
                ColumnMetaDTO metaDTO = new ColumnMetaDTO();
                metaDTO.setKey(record.getName());
                metaDTO.setType(record.getType().getName());
                metaDTOS.add(metaDTO);
            });
        } catch (KuduException e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
        return metaDTOS;
    }

    @Override
    protected ConnFactory getConnFactory() {
        return null;
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Kudu;
    }

    private static org.apache.kudu.client.KuduClient getConnection(SourceDTO source) {
        if (source == null || StringUtils.isBlank(source.getUrl())) {
            throw new DtCenterDefException("集群地址不能为空");
        }

        List<String> hosts = Arrays.stream(source.getUrl().split(",")).collect(Collectors.toList());
        return new org.apache.kudu.client.KuduClient.KuduClientBuilder(hosts).defaultOperationTimeoutMs(TIME_OUT).build();
    }

    /******************** 未支持的方法 **********************/
    @Override
    public Connection getCon(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<Map<String, Object>> executeQuery(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
