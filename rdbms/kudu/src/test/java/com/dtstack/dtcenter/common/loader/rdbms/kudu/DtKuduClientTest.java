package com.dtstack.dtcenter.common.loader.rdbms.kudu;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.apache.kudu.client.KuduClient;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

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
        SourceDTO source = SourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        List tableList = rdbsClient.getTableList(source, null);
        System.out.println(tableList);

    }

    @Test
    public void getColumnMetaData() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("impala::hxbho_pri.kudu100").build();
        List columnMetaData = rdbsClient.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData);

    }

    @Test
    public void getPreview() {

        SourceDTO source = SourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("impala::kududb.kudu_1").build();
        List list = rdbsClient.getPreview(source, queryDTO);
        System.out.println(list);


    }
}