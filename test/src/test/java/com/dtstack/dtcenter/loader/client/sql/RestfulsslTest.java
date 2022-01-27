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
public class RestfulsslTest extends BaseTest {

    // 构建 tsdb client
    private static final IRestful RESTFUL_CLIENT = ClientCache.getRestful(DataSourceType.RESTFUL.getVal());

    // 构建数据源信息
    private static final RestfulSourceDTO SOURCE_DTO = RestfulSourceDTO.builder()
            //.url("http://kudu1:8088/proxy/application_1639670432612_1380/#/job-manager/log")
            .url("https://172.16.89.2:20026/Yarn/NodeManager/23/node/containerlogs/container_e11_1642672613908_0205_01_000001/dtinsight/jobmanager.log/?start=0")
           // .sslClientConf(RestfulsslTest.class.getResource("/ssl").getPath()+"/ssl-client.xml")
            .build();


    @Test
    public void get() {
        Response response = RESTFUL_CLIENT.get(SOURCE_DTO, null, null, null);
        System.out.println(response.toString());
        Assert.assertNotNull(response.getContent());
    }
}
