package com.dtstack.dtcenter.loader.client;

import org.junit.BeforeClass;

/**
 * 设置基本参数
 *
 * @author ：wangchuan
 * date：Created in 下午4:54 2021/5/18
 * company: www.dtstack.com
 */
public class BaseTest {

    @BeforeClass
    public static void beforeTest() {
        String userDir = ClientCache.getUserDir();
        String coreDir = userDir.replace("/test/", "/core/");
        ClientCache.setUserDir(coreDir);
    }
}
