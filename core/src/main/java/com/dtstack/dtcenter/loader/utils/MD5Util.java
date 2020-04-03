package com.dtstack.dtcenter.loader.utils;

import com.dtstack.dtcenter.common.util.HexUtil;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:55 2020/3/6
 * @Description：MD5 工具类
 */
@Slf4j
public class MD5Util {
    /**
     * 获得文件的md5值
     *
     * @param file 需要加密的文件
     * @return md5加密后的字符串
     */
    public static String getMD5String(File file) {
        try (FileInputStream inputStream = new FileInputStream(file)) {
            return DigestUtils.md5Hex(inputStream);
        } catch (IOException e) {
            throw new DtLoaderException("文件获取 MD5 异常", e);
        }
    }

    /**
     * 获得字符串的md5值
     *
     * @param str 待加密的字符串
     * @return md5加密后的字符串
     */
    public static String getMD5String(String str) {
        byte[] bytes = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            bytes = md5.digest(str.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage(), e);
        } catch (NoSuchAlgorithmException e) {
            log.error(e.getMessage(), e);
        }
        return HexUtil.bytes2Hex(bytes);

    }
}
