package com.dtstack.dtcenter.common.loader.odps;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.loader.dto.source.OdpsSourceDTO;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2020/4/8
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
public class OdpsClientTest {
    private OdpsClient client = new OdpsClient();
    private OdpsSourceDTO sourceDTO;
    {
        Map<String,String> odpsConfig = new HashMap<>();
        odpsConfig.put("accessId", "LTAIljBeC8ei9Yy0");
        odpsConfig.put("accessKey", "gwTWasH7sEE0pSUEuiXnw7JecXyfGF");
        odpsConfig.put("project", "dtstack_dev");
        sourceDTO = OdpsSourceDTO.builder().config(JSON.toJSONString(odpsConfig)).build();
    }

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}