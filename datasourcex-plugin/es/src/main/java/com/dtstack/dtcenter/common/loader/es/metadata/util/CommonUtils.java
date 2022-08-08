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

package com.dtstack.dtcenter.common.loader.es.metadata.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.List;

/**
 * 通用util方法
 *
 * @author luming
 * @date 2022/4/13
 */
public class CommonUtils {

    /**
     * 处理es的url
     *
     * @param address es url
     * @return 处理过后的url
     */
    public static List<HttpHost> dealHost(String address) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = address.split(",");
        for (String add : addr) {
            String[] pair = add.split(":");
            httpHostList.add(new HttpHost(pair[0], Integer.parseInt(pair[1]), "http"));
        }
        return httpHostList;
    }

    /**
     * 集合转数组
     *
     * @param list 入参集合
     * @return 转换后的数组
     */
    public static HttpHost[] list2Arr(List<HttpHost> list) {
        if (CollectionUtils.isEmpty(list)) {
            return new HttpHost[0];
        }
        return list.toArray(new HttpHost[list.size()]);
    }
}
