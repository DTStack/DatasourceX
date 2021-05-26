package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.source.S3SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:28 2020/9/29
 * @Description：S3 测试
 */
@Ignore
public class S3Test extends BaseTest {
    // 没有可用的数据源
    S3SourceDTO source = S3SourceDTO.builder()
            .username("KERUE23F0YDYOR8AHSIZ")
            .password("IU3yNb1spyX1FfHWZsceeaeNDYCMpV7Mmd2C8IeI")
            .hostname("cdm-union-c01.eos-wuxi-1.cmecloud.cn")
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.S3.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getTable() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.S3.getPluginName());
        List<String> tables = client.getTableList(source, null);
        assert tables != null;
    }
}
