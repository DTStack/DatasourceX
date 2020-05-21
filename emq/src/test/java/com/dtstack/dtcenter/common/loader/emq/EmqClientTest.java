package com.dtstack.dtcenter.common.loader.emq;

import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

/**
 * Date: 2020/4/7
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
public class EmqClientTest {
    private static EmqClient absMQClient = new EmqClient();

    @Test
    public void testCon() {
        Boolean aBoolean = absMQClient.testCon(SourceDTO.builder().url("tcp://172.16.8.197:1883").build());
        System.out.println(aBoolean);
    }
}