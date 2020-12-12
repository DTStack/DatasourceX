package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.source.Greenplum6SourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.Test;

/**
 * greenplum table测试
 *
 * @author ：wangchuan
 * date：Created in 10:14 上午 2020/12/7
 * company: www.dtstack.com
 */
public class GreenplumTableTest {

    private static Greenplum6SourceDTO source = Greenplum6SourceDTO.builder()
            .url("jdbc:pivotal:greenplum://172.16.10.90:5432;DatabaseName=data")
            .username("gpadmin")
            .password("gpadmin")
            .schema("public")
            .build();

    /**
     * 重命名表
     */
    @Test
    public void renameTable () throws Exception {
        ITable client = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Boolean renameCheck1 = client.renameTable(source, "wangchuan_test2", "wangchuan_test3");
        Assert.assertTrue(renameCheck1);
        Boolean renameCheck2 = client.renameTable(source, "wangchuan_test3", "wangchuan_test2");
        Assert.assertTrue(renameCheck2);
    }
}
