package com.dtstack.dtcenter.loader.client.sql;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.common.enums.DataSourceClientType;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2020/4/9
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
public class OdpsTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();
    private SourceDTO sourceDTO;
    {
        Map<String,String> odpsConfig = new HashMap<>();
        odpsConfig.put("accessId", "LTAIljBeC8ei9Yy0");
        odpsConfig.put("accessKey", "gwTWasH7sEE0pSUEuiXnw7JecXyfGF");
        odpsConfig.put("project", "dtstack_dev");
        sourceDTO = SourceDTO.builder().config(JSON.toJSONString(odpsConfig)).build();
    }

    @Test
    public void testCon() {

        Boolean aBoolean = clientCache.getClient(DataSourceClientType.MAXCOMPUTE.getPluginName()).testCon(sourceDTO);
        System.out.println(aBoolean);
    }


}
