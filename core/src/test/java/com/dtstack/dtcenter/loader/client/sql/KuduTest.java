package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KuduSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
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

    // 构建client
    private static final IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());

    // 数据源信息
    private static final KuduSourceDTO source = KuduSourceDTO.builder()
            .url("172.16.100.109:7051")
            .build();

    private static final Pattern TABLE_COLUMN = Pattern.compile("(?i)schema.columns\\s*");

    @Test
    public void testCon() {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnMetaData() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("test_kudu").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    @Test
    public void testRegx(){
        String errorMessage = "error is : schema.columns[120]sass";
        Matcher passLine = TABLE_COLUMN.matcher(errorMessage);
        System.out.println(passLine.find());
    }


    /**
     * 数据预览，表没有有效的位置
     */
    @Test(expected = Exception.class)
    public void preview(){
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("test_kudu").build();
        List<ColumnMetaDTO> columnMetaData = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }
}
