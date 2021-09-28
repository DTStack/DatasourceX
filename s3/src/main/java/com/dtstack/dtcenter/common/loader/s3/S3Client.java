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

package com.dtstack.dtcenter.common.loader.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:01 2020/9/29
 * @Description：S3 客户端
 */
@Slf4j
public class S3Client<T> extends AbsNoSqlClient<T> {
    @Override
    public Connection getCon(ISourceDTO source) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Connection getCon(ISourceDTO source, String taskParams) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean testCon(ISourceDTO source) {
        AmazonS3Client amazonS3Client = null;
        try {
            amazonS3Client = S3Utils.getClient(source, null);
            amazonS3Client.listBuckets();
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }finally {
            if (amazonS3Client != null) {
                amazonS3Client.shutdown();
            }
        }
        return true;
    }

    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) {
        AmazonS3Client amazonS3Client = null;
        List<Bucket> buckets;
        try {
            amazonS3Client = S3Utils.getClient(source, null);
            buckets = amazonS3Client.listBuckets();
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }finally {
            if (amazonS3Client != null) {
                amazonS3Client.shutdown();
            }
        }
        if (CollectionUtils.isEmpty(buckets)) {
            return Collections.emptyList();
        }
        return buckets.stream().map(Bucket::getName).collect(Collectors.toList());
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        return getTableList(source, queryDTO);
    }
}
