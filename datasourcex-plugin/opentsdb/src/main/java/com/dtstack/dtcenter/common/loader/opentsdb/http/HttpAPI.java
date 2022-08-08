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

package com.dtstack.dtcenter.common.loader.opentsdb.http;

/**
 * OpenTSDB Http API 接口
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/17
 * company: www.dtstack.com
 */
public interface HttpAPI {

    String PUT = "/api/put";

    String QUERY = "/api/query";

    String DELETE_DATA = "/api/delete_data";

    String DELETE_META = "/api/delete_meta";

    String SUGGEST = "/api/suggest";

    String VERSION = "/api/version";

    String VIP_HEALTH = "/api/vip_health";
}