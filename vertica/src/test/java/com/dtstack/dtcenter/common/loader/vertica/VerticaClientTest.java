package com.dtstack.dtcenter.common.loader.vertica;

import com.dtstack.dtcenter.loader.dto.source.VerticaSourceDTO;
import org.junit.Test;

public class VerticaClientTest {
    @Test
    public void testCon() throws Exception {
        VerticaClient verticaClient = new VerticaClient();
        VerticaSourceDTO verticaSourceDTO = VerticaSourceDTO.builder()
                .url("jdbc:vertica://172.16.101.225:5433/docker")
                .username("dbadmin")
                .build();
        verticaClient.getTableList(verticaSourceDTO, null);
    }
}