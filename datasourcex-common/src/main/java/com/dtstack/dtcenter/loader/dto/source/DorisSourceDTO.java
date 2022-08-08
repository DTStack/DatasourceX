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

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.Map;

/**
 * 由于metadata获取数据时需要jdbc和http两种方式，故这里额外添加了http的连接参数
 */
@EqualsAndHashCode(callSuper = true)
@Data
@ToString
@SuperBuilder
public class DorisSourceDTO extends Mysql5SourceDTO {

    private DorisRestfulSourceDTO restfulSource;

    @Override
    public Integer getSourceType() {
        return DataSourceType.DORIS.getVal();
    }
}
