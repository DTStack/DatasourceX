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

package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Connection;

/**
 * aws s3 sourceDTO
 *
 * @author ：wangchuan
 * date：Created in 上午9:51 2021/5/6
 * company: www.dtstack.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AwsS3SourceDTO implements ISourceDTO {

    /**
     * aws s3 文件访问密钥
     */
    private String accessKey;

    /**
     * aws s3 密钥
     */
    private String secretKey;

    /**
     * 桶所在区
     */
    private String region;

    @Override
    public Integer getSourceType() {
        return DataSourceType.AWS_S3.getVal();
    }

    @Override
    public String getUsername() {
        throw new DtLoaderException("This method is not supported");
    }

    @Override
    public String getPassword() {
        throw new DtLoaderException("This method is not supported");
    }

    @Override
    public Connection getConnection() {
        throw new DtLoaderException("This method is not supported");
    }

    @Override
    public void setConnection(Connection connection) {
        throw new DtLoaderException("This method is not supported");
    }
}
