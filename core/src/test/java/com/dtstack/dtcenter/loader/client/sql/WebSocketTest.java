package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.source.WebSocketSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.util.HashMap;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:24 2020/11/25
 * @Description：WebSocket 测试类
 */
public class WebSocketTest {

    @Test
    public void testCon() throws Exception {
        HashMap<String, String> map = new HashMap<>();
        map.put("userName", "nanqi");
        WebSocketSourceDTO sourceDTO = WebSocketSourceDTO.builder().url("ws://127.0.0.1:8080/nanqi").authParams(map).build();
        IClient client = ClientCache.getClient(DataSourceType.WEB_SOCKET.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }
}
