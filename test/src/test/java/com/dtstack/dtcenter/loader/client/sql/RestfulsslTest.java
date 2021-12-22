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
            .url("https://cdp02:8090/proxy/application_1639383661552_0958/#/job-manager/logs")
            .sslClientConf(RestfulsslTest.class.getResource("/ssl").getPath()+"/ssl-client.xml")
            .build();
}
