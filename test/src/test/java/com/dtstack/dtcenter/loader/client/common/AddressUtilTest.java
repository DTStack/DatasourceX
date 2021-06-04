package com.dtstack.dtcenter.loader.client.common;

import com.dtstack.dtcenter.common.loader.common.utils.AddressUtil;
import com.dtstack.dtcenter.loader.client.testutil.ReflectUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.List;
import java.util.Set;

/**
 * 地址工具类测试
 *
 * @author ：wangchuan
 * date：Created in 上午10:31 2021/6/4
 * company: www.dtstack.com
 */
public class AddressUtilTest {

    /**
     * 获取本地 ip 地址
     */
    @Test
    public void resolveLocalAddressesTest() {
        Set<InetAddress> addresses = AddressUtil.resolveLocalAddresses();
        Assert.assertTrue(CollectionUtils.isNotEmpty(addresses));
    }

    /**
     * 判断是否是本机地址
     */
    @Test
    public void isSpecialIpTest() {
        Assert.assertTrue(ReflectUtil.invokeStaticMethod(AddressUtil.class, Boolean.class, "isSpecialIp", new Class[]{String.class}, ":"));
        Assert.assertTrue(ReflectUtil.invokeStaticMethod(AddressUtil.class, Boolean.class, "isSpecialIp", new Class[]{String.class}, "127.0.0.1"));
        Assert.assertTrue(ReflectUtil.invokeStaticMethod(AddressUtil.class, Boolean.class, "isSpecialIp", new Class[]{String.class}, "169.254.0.1"));
        Assert.assertTrue(ReflectUtil.invokeStaticMethod(AddressUtil.class, Boolean.class, "isSpecialIp", new Class[]{String.class}, "255.255.255.255"));
        Assert.assertFalse(ReflectUtil.invokeStaticMethod(AddressUtil.class, Boolean.class, "isSpecialIp", new Class[]{String.class}, "8.8.8.8"));
    }

    /**
     * 获取本机的第一个 ip
     */
    @Test
    public void getOneIP() {
        String oneIP = AddressUtil.getOneIP();
        Assert.assertTrue(StringUtils.isNotBlank(oneIP));
    }

    /**
     * 获取本地 ip
     */
    @Test
    public void resolveLocalIps() {
        List<String> ips = AddressUtil.resolveLocalIps();
        Assert.assertTrue(CollectionUtils.isNotEmpty(ips));
    }

    /**
     * telnet ip port
     */
    @Test
    public void telnet() {
        Assert.assertTrue(AddressUtil.telnet("172.16.100.251", 9023));
        Assert.assertFalse(AddressUtil.telnet("0.0.0.0", 8080));
    }

    /**
     * ping ip
     */
    @Test
    public void ping() {
        Assert.assertTrue(AddressUtil.ping("172.16.100.251"));
        Assert.assertFalse(AddressUtil.ping("1.1.1.1"));
    }

    /**
     * 校验 ip 是否是 '0.0.0.0', '127.0.0.1'
     */
    @Test
    public void checkAddrIsLocal() {
        Assert.assertTrue(AddressUtil.checkAddrIsLocal("0.0.0.0"));
        Assert.assertFalse(AddressUtil.ping("0.0.0.1"));
    }

    /**
     * 检查服务是否相同：ip相同，端口相同
     */
    @Test
    public void checkServiceIsSame() throws Exception {
        Assert.assertTrue(AddressUtil.checkServiceIsSame("172.16.100.251", 80, "172.16.100.251", 80));
        Assert.assertFalse(AddressUtil.checkServiceIsSame("172.16.100.251", 80, "172.16.100.251", 81));
    }
}
