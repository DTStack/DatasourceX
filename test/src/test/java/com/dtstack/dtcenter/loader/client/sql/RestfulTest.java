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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IRestful;
import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.dto.source.RestfulSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Maps;
import org.apache.commons.math3.util.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * restful 测试类
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/7
 * company: www.dtstack.com
 */
public class RestfulTest extends BaseTest {

    // 构建client
    private static final IClient CLIENT = ClientCache.getClient(DataSourceType.RESTFUL.getVal());

    // 构建数据源信息
    private static final RestfulSourceDTO SOURCE_DTO = RestfulSourceDTO.builder()
            .url("https://www.baidu.com/")
            .build();

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        Boolean isConnected = CLIENT.testCon(SOURCE_DTO);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }


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

    @Test
    public void get() {
        SOURCE_DTO.setUrl("http://172.16.21.49:8030/api/meta/namespaces/default_cluster/databases");
        Map<String, String> params = Maps.newHashMap();
        params.put("timespan", "100m");
        Response response = RESTFUL_CLIENT.get(SOURCE_DTO, params, null, null);
        Assert.assertNotNull(response.getContent());
    }


    @Test
    public void post() {
        SOURCE_DTO.setUrl("http://172.16.21.49:8030/api/meta/namespaces/default_cluster/databases");
        Response response = RESTFUL_CLIENT.get(SOURCE_DTO, null, null, null);
        Assert.assertNotNull(response.getContent());
        System.out.println(response.getContent());
    }

    @Test
    public void get1() {
        SOURCE_DTO.setUrl("http://172.16.21.49:8030/api/meta/namespaces/default_cluster/databases/qianyi/tables/LOADER_TEST/schema");
        Response response = RESTFUL_CLIENT.get(SOURCE_DTO, null, null, null);
        Assert.assertNotNull(response.getContent());
        System.out.println(response.getContent());
    }

    @Test
    public void cc(){

        // String json = "{\"msg\":\"success\",\"code\":0,\"data\":{\"LOADER_TEST\":{\"schema\":[{\"Field\":\"id\",\"Type\":\"INT\",\"Null\":\"Yes\",\"Extra\":\"\",\"Default\":null,\"Key\":\"true\"},{\"Field\":\"name\",\"Type\":\"VARCHAR(50)\",\"Null\":\"Yes\",\"Extra\":\"\",\"Default\":null,\"Key\":\"true\"}],\"is_base\":true}},\"count\":0}";
        String json = "{\"msg\":\"success\",\"code\":0,\"data\":{\"LOADER_TEST\":{\"schema\":[],\"is_base\":true}},\"count\":0}";

        JSONArray object = (JSONArray) JSONPath.eval(JSONObject.parse(json), "$.data.LOADER_TEST.schema");
        System.out.println(object);
    }

    @Test
    public void dd() {
        String json = "{\"msg\":\"success\",\"code\":0,\"data\":[\"default_cluster:StreamData\",\"default_cluster:dujie\",\"default_cluster:information_schema\",\"default_cluster:kunka\",\"default_cluster:ods_streamdata_end\",\"default_cluster:ods_testcase_end\",\"default_cluster:ods_tiezhu\",\"default_cluster:qianyi\",\"default_cluster:streamdata\",\"default_cluster:testOne\",\"default_cluster:testcase\",\"default_cluster:tiezhu\",\"default_cluster:xiaohe_db\"],\"count\":0}";
        JSONArray object = (JSONArray)JSONPath.eval(JSONObject.parse(json), "$.data");
        List<String> list = object.stream().map(obj -> ((String)obj).substring(((String)obj).lastIndexOf(":") + 1)).collect(Collectors.toList());
        System.out.println(list);
    }

    @Test
    public void ee() {
        SOURCE_DTO.setUrl("http://172.16.21.49:8030/api/meta/namespaces/default_cluster/databases/qianyi/tables");
        Response response = RESTFUL_CLIENT.get(SOURCE_DTO, null, null, null);
        Assert.assertNotNull(response.getContent());
        System.out.println(response.getContent());
    }




    @Test
    public void ff() {
        SOURCE_DTO.setUrl("http://172.16.21.49:8030/api/query/default_cluster/qianyi");
        String body = "{stmt: \"select * from LOADER_TEST;;\"}";
        Response response = RESTFUL_CLIENT.post(SOURCE_DTO, body, null, null);
        Assert.assertNotNull(response.getContent());
        System.out.println(response.getContent());
    }


    private static final String DATA_JSON_PATH = "$.data.data";

    private static final String META_JSON_PATH = "$.data.meta";

    @Test
    public void gg() {
        String json = "{\n" +
                "    \"msg\":\"success\",\n" +
                "    \"code\":0,\n" +
                "    \"data\":{\n" +
                "        \"data\":[\n" +
                "            [\n" +
                "                1,\n" +
                "                \"LOADER_TEST\"\n" +
                "            ]\n" +
                "        ],\n" +
                "        \"meta\":[\n" +
                "            {\n" +
                "                \"name\":\"id\",\n" +
                "                \"type\":\"INT\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"name\":\"name\",\n" +
                "                \"type\":\"CHAR\"\n" +
                "            }\n" +
                "        ],\n" +
                "        \"time\":40,\n" +
                "        \"type\":\"result_set\"\n" +
                "    },\n" +
                "    \"count\":0\n" +
                "}";
        JSONArray data = (JSONArray) JSONPath.eval(JSONObject.parse(json), DATA_JSON_PATH);
        JSONArray metaObj = (JSONArray) JSONPath.eval(JSONObject.parse(json), META_JSON_PATH);
        List<String> meta = metaObj.stream().map(obj -> ((JSONObject) obj).getString("name")).collect(Collectors.toList());
        List<List<Object>> resultList = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            List<Object> line = new ArrayList<>();
            JSONArray jsonObject = data.getJSONArray(i);
            for (int j = 0; j < meta.size(); j++)
                line.add(new Pair<String, Object>(meta.get(j), jsonObject.get(j)));
            resultList.add(line);
        }
        System.out.println(resultList);
    }



}
