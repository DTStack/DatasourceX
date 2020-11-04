package com.dtstack.dtcenter.common.loader.kudu;

import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KuduSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:00 2020/2/27
 * @Description：Kudu 客户端
 */
@Slf4j
public class DtKuduClient<T> implements IClient<T> {

    private static final int TIME_OUT = 5 * 1000;
    private static int PRE_SIZE = 3;

    private static final Pattern TABLE_COLUMN = Pattern.compile("(?i)schema.columns\\s*");

    private static final String COLUMN_TYPE_NOT_SUPPORT= "DECIMAL、CHAR、VARCHAR、DATE";


    @Override
    public Boolean testCon(ISourceDTO iSource) {
        KuduSourceDTO kuduSourceDTO = (KuduSourceDTO) iSource;
        if (null == kuduSourceDTO || StringUtils.isBlank(kuduSourceDTO.getUrl())) {
            return false;
        }
        try (KuduClient client = getConnection(kuduSourceDTO)){
                client.getTablesList();
            return true;
        } catch (KuduException e) {
            log.error("检查连接异常", e);
        }
        return false;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        List<String> tableList = null;
        try (KuduClient client = getConnection(iSource);){
            tableList = client.getTablesList().getTablesList();
        } catch (KuduException e) {
            log.error(e.getMessage(), e);
        }
        return tableList;
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        if (queryDTO == null || StringUtils.isBlank(queryDTO.getTableName())) {
            throw new DtLoaderException("表名称不能为空");
        }
        try (KuduClient client = getConnection(iSource);) {
            return getTableColumns(client, queryDTO.getTableName());
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

            throw new DtLoaderException(dealMessageError(e.getMessage()), e);
        }
        return metaDTOS;
    }

    private String dealMessageError(String errorMessage){
        Matcher passLine = TABLE_COLUMN.matcher(errorMessage);
        if (passLine.find()) {
           return String.format("请校验字段类型,kudu表暂时不持%s等类型",COLUMN_TYPE_NOT_SUPPORT);
        }
        return errorMessage;
    }

    private static KuduClient getConnection(ISourceDTO iSource) {
        KuduSourceDTO kuduSourceDTO = (KuduSourceDTO) iSource;
        if (kuduSourceDTO == null || StringUtils.isBlank(kuduSourceDTO.getUrl())) {
            throw new DtLoaderException("集群地址不能为空");
        }
        List<String> hosts = Arrays.stream(kuduSourceDTO.getUrl().split(",")).collect(Collectors.toList());
        log.info("获取 Kudu 数据源连接, url : {}, kerberosConfig : {}", hosts, kuduSourceDTO.getKerberosConfig());
        if (MapUtils.isEmpty(kuduSourceDTO.getKerberosConfig())) {
            return new KuduClient.KuduClientBuilder(hosts).defaultOperationTimeoutMs(TIME_OUT).build();
        }

        return KerberosLoginUtil.loginKerberosWithUGI(kuduSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<KuduClient>) () -> new KuduClient.KuduClientBuilder(hosts).defaultOperationTimeoutMs(TIME_OUT).build()
        );
    }


    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        if (StringUtils.isBlank(queryDTO.getTableName())) {
            return null;
        }

        KuduClient client = null;
        KuduScanner scanner = null;
        List<List<Object>> dataList = new ArrayList<>();
        try {
            client = getConnection(source);
            KuduTable kuduTable = client.openTable(queryDTO.getTableName());
            Schema schema = kuduTable.getSchema();
            List<String> columnStr = schema.getColumns().stream().map(ColumnSchema::getName).collect(Collectors.toList());
            KuduScanner.KuduScannerBuilder scannerBuilder = client.newScannerBuilder(kuduTable)
                    .setProjectedColumnNames(columnStr)
                    .cacheBlocks(false)
                    .readMode(AsyncKuduScanner.ReadMode.READ_LATEST)
                    .batchSizeBytes(1024)
                    .limit(PRE_SIZE)
                    .scanRequestTimeout(TIME_OUT);
            scanner = scannerBuilder.build();
            int tempSize = PRE_SIZE;
            while(scanner.hasMoreRows() && tempSize > 0){
                RowResultIterator curRows = scanner.nextRows();
                while(curRows.hasNext() && tempSize-- > 0){
                    RowResult rowResult = curRows.next();
                    List<Object> row = findRow(schema,rowResult);
                    dataList.add(row);
                }
            }
        } catch (KuduException e) {
            throw new DtLoaderException(dealMessageError(e.getMessage()), e);
        } finally {
            closeClient(client, null, scanner);
        }
        return dataList;
    }

    private static List<Object> findRow(Schema schema,RowResult rowResult){
        List<Object> row = new ArrayList<>();
        for(ColumnSchema columnSchema : schema.getColumns()){
            if(rowResult.isNull(columnSchema.getName())){
                row.add(null);
            }else{
                switch(columnSchema.getType()){
                    case INT8:
                        row.add(rowResult.getByte(columnSchema.getName()));
                        break;
                    case INT16:
                        row.add(rowResult.getShort(columnSchema.getName()));
                        break;
                    case INT32:
                        row.add(rowResult.getInt(columnSchema.getName()));
                        break;
                    case INT64:
                    case UNIXTIME_MICROS:
                        //由于long值返回前端进行json转换会丢失精度，所以转换为字符串返回
                        row.add(String.valueOf(rowResult.getLong(columnSchema.getName())));
                        break;
                    case BINARY:
                        row.add("[BINARY]");
                        break;
                    case STRING:
                        row.add(rowResult.getString(columnSchema.getName()));
                        break;
                    case BOOL:
                        row.add(rowResult.getBoolean(columnSchema.getName()));
                        break;
                    case FLOAT:
                        row.add(rowResult.getFloat(columnSchema.getName()));
                        break;
                    case DOUBLE:
                        row.add(rowResult.getDouble(columnSchema.getName()));
                        break;
                    default:
                        row.add(rowResult.getString(columnSchema.getName()));
                        break;
                }
            }
        }
        return row;
    }


    public static void closeClient(org.apache.kudu.client.KuduClient client, KuduSession kuduSession, KuduScanner kuduScanner) {
        try {
            if (kuduScanner != null) {
                kuduScanner.close();
            }
            if (kuduSession != null) {
                kuduSession.close();
            }
            if (client != null) {
                client.close();
            }
        } catch (KuduException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        return getColumnMetaData(source, queryDTO);
    }

    /******************** 未支持的方法 **********************/
    @Override
    public Connection getCon(ISourceDTO iSource) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getTableMetaComment(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Table getTable(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }
}
