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
import com.dtstack.dtcenter.loader.client.IRestful;
import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.dto.source.RestfulSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Map;

/**
 * Restful 测试类
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/7
 * company: www.dtstack.com
 */
public class RestfulSpecialTest extends BaseTest {

    // 构建 tsdb client
    private static final IRestful RESTFUL_CLIENT = ClientCache.getRestful(DataSourceType.RESTFUL.getVal());

    @BeforeClass
    public static void setUp() {
        Map<String, String> headers = Maps.newHashMap();
        headers.put("Cookie", "experimentation_subject_id=IjJmODU3NDA2LTBkNzYtNGNiYy04N2IwLWNlMWM3ZjQzMWNjZCI%3D--5e2445f667ca5d811f632ba88c6d72c0ef3fc7fd; dt_product_code=RDOS; _ga=GA1.2.71000767.1589853613; _rdt_uuid=1627373466378.20491750-ac1e-4008-98f7-78eadbb17e12; _fbp=fb.1.1627373467354.674167407; sysLoginType=%7B%22sysId%22%3A1%2C%22sysType%22%3A0%7D; dt_user_id=1; dt_username=admin%40dtstack.com; dt_can_redirect=false; dt_cookie_time=2021-08-12+09%3A59%3A35; dt_tenant_id=1; dt_tenant_name=DTStack%E7%A7%9F%E6%88%B7; dt_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0ZW5hbnRfaWQiOiIxIiwidXNlcl9pZCI6IjEiLCJ1c2VyX25hbWUiOiJhZG1pbkBkdHN0YWNrLmNvbSIsImV4cCI6MTY1OTU3ODM3NSwiaWF0IjoxNjI4NDc0Mzg0fQ.BnAaGZy8kdslpY3IRVCkgIuPrRQ3ELRR4UyiOsu0gr0; dt_is_tenant_admin=true; dt_is_tenant_creator=false; SESSION=Y2I1MzQ5ZGQtNmZlYi00YjIzLTkwMTktNjBjZDJiM2VkYWRi; JSESSIONID=471B2B3F473C0DC056C8CADB844F5806; DT_SESSION_ID=f3083f44-69b6-4d3e-8c75-55c5ad4ff5ec");
        headers.put("Host", "dev.insight.dtstack.cn");
        headers.put("Origin", "http://dev.insight.dtstack.cn");
        headers.put("Referer", "http://dev.insight.dtstack.cn/stream/");
        headers.put("X-Project-ID", "209");
        SOURCE_DTO.setHeaders(headers);
    }

    // 构建数据源信息
    private static final RestfulSourceDTO SOURCE_DTO = RestfulSourceDTO.builder()
            .url("http://dev.insight.dtstack.cn/api/streamapp/service/streamDataSource/pageQuery")
            .build();

    @Test
    public void get() {
        SOURCE_DTO.setUrl("http://dev.insight.dtstack.cn/api/streamapp/service/streamJobMetric/formatTimespan");
        Map<String, String> params = Maps.newHashMap();
        params.put("timespan", "100m");
        Response response = RESTFUL_CLIENT.get(SOURCE_DTO, params, null, null);
        Assert.assertNotNull(response.getContent());
    }

    @Test
    public void post() {
        SOURCE_DTO.setUrl("http://dev.insight.dtstack.cn/api/streamapp/service/streamDataSource/pageQuery");
        Response response = RESTFUL_CLIENT.post(SOURCE_DTO, "{\"pageSize\":20,\"currentPage\":1,\"name\":\"\",\"types\":[]}", null, null);
        Assert.assertNotNull(response.getContent());
    }

    @Test
    public void postMultipart() {
        SOURCE_DTO.setUrl("http://dev.insight.dtstack.cn/api/streamapp/upload/streamResource/addResource");
        Map<String, File> files = Maps.newHashMap();
        File file = new File(RestfulSpecialTest.class.getResource("/restful").getPath() + "/loader_test.jar");
        files.put("file", file);
        Map<String, String> params = Maps.newHashMap();
        params.put("originFileName", "kafka_consume.jar");
        params.put("resourceDesc", "test_loader");
        params.put("nodePid", "1467");
        params.put("id", "7959");
        Response response = RESTFUL_CLIENT.postMultipart(SOURCE_DTO, params, null, null, files);
        Assert.assertNotNull(response.getContent());
    }
}
