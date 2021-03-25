package com.dtstack.dtcenter.common.loader.common.utils;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:47 2020/2/26
 * @Description：Tel 工具类
 */
public class TelUtil {

    // ip:端口 正则解析，适配ipv6
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>(.*)):(?<port>\\d+)*");

    public static boolean checkTelnetAddr(String urls) {
        boolean result = false;
        String[] addrs = urls.split(",");
        for (String addr : addrs) {
            Matcher matcher = HOST_PORT_PATTERN.matcher(addr);
            if (!matcher.find()) {
                throw new DtLoaderException(String.format("地址：%s 格式错误", addr));
            }
            String host = matcher.group("host");
            String portStr = matcher.group("port");
            if (StringUtils.isBlank(host) || StringUtils.isBlank(portStr)) {
                throw new DtLoaderException(String.format("地址：%s 缺少ip或端口", addr));
            }
            result = AddressUtil.telnet(host.trim(), Integer.parseInt(portStr.trim()));
            if (!result) {
                throw new DtLoaderException(String.format("地址：%s 无法连通", addr));
            }
        }
        return result;
    }
}
