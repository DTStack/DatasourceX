package com.dtstack.dtcenter.common.loader.rdbms.greenplum;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 19:45 2020/4/13
 * @Description：TODO
 */
public class GreenplumClientTest {
    private static AbsRdbmsClient rdbsClient = new GreenplumClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:pivotal:greenplum://172.16.10.90:5432;DatabaseName=data")
                .username("gpadmin")
                .password("gpadmin")
                .schema("data")
                .build();
        List<String> tableList = rdbsClient.getTableList(source, null);
        assert tableList != null;
    }
}
