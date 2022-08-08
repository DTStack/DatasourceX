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

package com.dtstack.dtcenter.common.loader.hbase.restful.entity;

import lombok.Builder;
import lombok.Data;

/**
 * 过滤器对象
 *
 * @author luming
 * @date 2022/5/7
 */
@Builder
@Data
public class FilterDTO {
    /**
     * 过滤器类型
     */
    private String type;
    /**
     * 比较运算符
     */
    private String op;
    /**
     * 比较器
     */
    private Comparator comparator;
    /**
     * 比较值
     */
    private String value;
    /**
     * 列簇
     */
    private String family;
    /**
     * 列
     */
    private String qualifier;

    @Data
    @Builder
    public static class Comparator {
        private String type;
        private String value;
    }
}
