package com.dtstack.dtcenter.loader.utils;

import com.dtstack.dtcenter.loader.utils.MD5Util;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:23 2020/3/6
 * @Description：MD5 工具类
 */
@Slf4j
public class MD5Test {
    public static void main(String[] args) {
        long currentTimeMillis = System.currentTimeMillis();
        File file = new File("/Users/jialongyan/Desktop/temp_001/runner-test_3.10.x-with-dependencies1.jar");
        String md5String = MD5Util.getMD5String(file);
        System.out.println(System.currentTimeMillis() - currentTimeMillis);
    }
}
