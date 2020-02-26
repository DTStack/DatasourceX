package com.dtstack.dtcenter.loader.utils;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.util.AddressUtil;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:47 2020/2/26
 * @Description：Tel 工具类
 */
public class TelUtil {
    public static boolean checkTelnetAddr(String urls) {
        boolean result = false;
        String[] addrs = urls.split(",");
        for (String addr : addrs) {
            String[] ad = addr.split(":", 2);
            if (ad.length != 2) {
                throw new DtCenterDefException(addr + "地址格式错误", DBErrorCode.IP_PORT_FORMAT_ERROR);
            }
            String ip = ad[0].trim();
            String port = ad[1].trim();
            if (port.contains("/")) {
                port = port.substring(0, port.indexOf("/"));
            }
            result = AddressUtil.telnet(ip, Integer.parseInt(port));
            if (!result) {
                throw new DtCenterDefException(addr + "无法联通", DBErrorCode.IP_PORT_CONN_ERROR);
            }
        }
        return result;
    }
}
