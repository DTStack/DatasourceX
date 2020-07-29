package com.dtstack.dtcenter.common.loader.kudu;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KuduSourceDTO;
import org.junit.Test;

import java.util.List;

/**
 * Date: 2020/4/8
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
public class DtKuduClientTest {
    private static AbsRdbmsClient rdbsClient = new DtKuduClient();

    @Test
    public void getConnFactory() throws Exception {
        KuduSourceDTO source = KuduSourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        KuduSourceDTO source = KuduSourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        List tableList = rdbsClient.getTableList(source, null);
        System.out.println(tableList);

    }

    @Test
    public void getColumnMetaData() throws Exception {
        KuduSourceDTO source = KuduSourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("impala::hxbho_pri.kudu100").build();
        List columnMetaData = rdbsClient.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData);

    }

    @Test
    public void getPreview() throws Exception {
        KuduSourceDTO source = KuduSourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("impala::kududb.kudu_1").build();
        List list = rdbsClient.getPreview(source, queryDTO);
        System.out.println(list);
    }

}