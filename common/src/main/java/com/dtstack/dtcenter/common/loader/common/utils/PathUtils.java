package com.dtstack.dtcenter.common.loader.common.utils;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:24 2020/8/28
 * @Description：文件路径处理
 */
public class PathUtils {
    /**
     * 处理路径中存在多个分隔符的情况
     *
     * @param path
     * @return
     */
    public static String removeMultiSeparatorChar(String path) {
        return path.replaceAll("//*", "/");
    }
}
