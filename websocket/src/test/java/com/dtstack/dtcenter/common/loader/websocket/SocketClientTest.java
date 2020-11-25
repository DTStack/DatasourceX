package com.dtstack.dtcenter.common.loader.websocket;

import com.dtstack.dtcenter.loader.dto.source.WebSocketSourceDTO;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class SocketClientTest {

    @Test
    public void testCon() throws Exception {
        SocketClient socketClient = new SocketClient();
        HashMap<String, String> map = new HashMap<>();
        map.put("userName", "nanqi");
        WebSocketSourceDTO sourceDTO = WebSocketSourceDTO.builder().url("ws://127.0.0.1:8080/nanqi").authParams(map).build();
        Boolean testCon = socketClient.testCon(sourceDTO);
        Assert.assertEquals(Boolean.TRUE, testCon);
    }
}