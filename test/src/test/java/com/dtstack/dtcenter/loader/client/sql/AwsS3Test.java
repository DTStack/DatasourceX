/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
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
public class AwsS3Test extends BaseTest {

    // 获取数据源 client
    private static final IClient CLIENT = ClientCache.getClient(DataSourceType.AWS_S3.getVal());

    private static final AwsS3SourceDTO SOURCE_DTO = AwsS3SourceDTO.builder()
            .accessKey("AKIAVCE7XHFIL4ZA3652")
            .secretKey("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")
            .region("cn-northwest-1")
            .build();

    @Test
    public void testCon() {
        Assert.assertTrue(CLIENT.testCon(SOURCE_DTO));
    }

    @Test
    public void listBuckets() {
        List<String> buckets = CLIENT.getAllDatabases(SOURCE_DTO, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(buckets));
    }

    @Test
    public void listBucketsSearchLimit() {
        List<String> buckets = CLIENT.getAllDatabases(SOURCE_DTO, SqlQueryDTO.builder().schema("s3").limit(3).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(buckets));
    }

    @Test
    public void listObjectsByBucket() {
        List<String> buckets = CLIENT.getTableList(SOURCE_DTO, SqlQueryDTO.builder().schema("s3-smartdi-01").tableNamePattern("dt.*").limit(3).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(buckets));
    }
}
