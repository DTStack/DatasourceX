package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.AwsS3SourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * aws s3 test
 *
 * @author ：wangchuan
 * date：Created in 上午11:21 2021/5/6
 * company: www.dtstack.com
 */
public class AwsS3Test {

    // 获取数据源 client
    private static final IClient CLIENT = ClientCache.getClient(DataSourceType.AWS_S3.getVal());

    private static final AwsS3SourceDTO SOURCE_DTO = AwsS3SourceDTO.builder()
            .accessKey("AKIAVCE7XHFIL4ZA3652")
            .secretKey("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")
            .region("cn-north-1")
            .build();

    @Test
    public void testCon() {
        Assert.assertTrue(CLIENT.testCon(SOURCE_DTO));
    }

    @Test
    public void listBuckets() {
        List<String> buckets = CLIENT.getTableList(SOURCE_DTO, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(buckets));
    }
}
