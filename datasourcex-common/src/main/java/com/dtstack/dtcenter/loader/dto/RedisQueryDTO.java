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

package com.dtstack.dtcenter.loader.dto;

import com.dtstack.dtcenter.loader.enums.RedisCompareOp;
import com.dtstack.dtcenter.loader.enums.RedisDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;

/**
 * redis queryDTO
 *
 * @author ：qianyi
 * date：Created in 上午15:02 2021/9/8
 * company: www.dtstack.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisQueryDTO {

    /**
     * redis，模糊查询限制key的条数
     */
    private Integer keyLimit;

    /**
     * redis，查询结果条数限制， 只针对list,zset
     */
    private Integer ResultLimit;

    /**
     * redis 数据类型
     */
    @NonNull
    private RedisDataType redisDataType;

    /**
     * 操作符,= or like
     */
    private RedisCompareOp redisCompareOp;

    /**
     * keys，
     */
    private List<String> keys;


    /**
     * keys 模糊查询
     */
    private String keyPattern;

}
