package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KuduSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 01:35 2020/2/29
 * @Description：Kudu 测试
 */
public class KuduTest {
    KuduSourceDTO source = KuduSourceDTO.builder()
            .url("172.16.101.13:7051,172.16.100.105:7051,172.16.100.132:7051")
            .build();

    private static final Pattern TABLE_COLUMN = Pattern.compile("(?i)schema.columns\\s*");


    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("impala::default.nanqi01").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void testRegx(){
        String errorMessage = "error is : schema.columns[120]sass";
        Matcher passLine = TABLE_COLUMN.matcher(errorMessage);
        System.out.println(passLine.find());
    }
}
