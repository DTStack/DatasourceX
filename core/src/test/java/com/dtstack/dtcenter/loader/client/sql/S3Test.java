package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.source.S3SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:28 2020/9/29
 * @Description：S3 测试
 */
public class S3Test {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    S3SourceDTO source = S3SourceDTO.builder()
            .username("KERUE23F0YDYOR8AHSIZ")
            .password("IU3yNb1spyX1FfHWZsceeaeNDYCMpV7Mmd2C8IeI")
            .hostname("cdm-union-c01.eos-wuxi-1.cmecloud.cn")
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.S3.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void getTable() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.S3.getPluginName());
        List<String> tables = client.getTableList(source, null);
        assert tables != null;
    }
}
