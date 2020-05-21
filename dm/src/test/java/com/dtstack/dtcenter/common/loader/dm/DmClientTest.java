package com.dtstack.dtcenter.common.loader.dm;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

import java.util.List;

/**
 * Date: 2020/4/19
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
public class DmClientTest {
    private static AbsRdbmsClient rdbsClient = new DmClient();
    SourceDTO source = SourceDTO.builder()
            .url("jdbc:dm://172.16.8.178:5236/chener")
            .username("chener")
            .password("abc123456")
            .build();

    SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();


    @Test
    public void testCon() {
        Boolean aBoolean = rdbsClient.testCon(source);
        System.out.println(aBoolean);
    }

    @Test
    public void getTableList() throws Exception {
        source.setSchema("");
        List tableList = rdbsClient.getTableList(source, queryDTO);
        System.out.println(tableList);

    }

    @Test
    public void getColumnMetaData() {
    }
}