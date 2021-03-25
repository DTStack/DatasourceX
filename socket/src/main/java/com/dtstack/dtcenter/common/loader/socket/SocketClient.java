package com.dtstack.dtcenter.common.loader.socket;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SocketSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * socket数据源客户端
 *
 * @author ：wangchuan
 * date：Created in 4:16 下午 2020/12/28
 * company: www.dtstack.com
 */
@Slf4j
public class SocketClient<T> extends AbsNoSqlClient<T> {

    // ip:port正则
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>(.*)):((?<port>\\d+))*");

    @Override
    public Boolean testCon(ISourceDTO source) {
        SocketSourceDTO socketSourceDTO = (SocketSourceDTO) source;
        String hostPort = socketSourceDTO.getHostPort();
        if (StringUtils.isBlank(hostPort)) {
            throw new DtLoaderException("socket数据源ip和端口不能为空");
        }
        Matcher matcher = HOST_PORT_PATTERN.matcher(hostPort);
        Socket socket = null;
        if (matcher.find()) {
            String host = matcher.group("host");
            String portStr = matcher.group("port");
            if (StringUtils.isBlank(portStr)) {
                throw new DtLoaderException("socket数据源端口不能为空");
            }
            // 转化为int格式的端口
            int port = Integer.parseInt(portStr);
            try {
                // 方法内支持ipv6
                InetAddress address = InetAddress.getByName(host);
                socket = new Socket(address, port);
                // 往输出流发送一个字节的数据，Socket的SO_OOBINLINE属性没有打开，就会自动舍弃这个字节，该属性默认关闭
                socket.sendUrgentData(0xFF);
            } catch (UnknownHostException e) {
                throw new DtLoaderException(String.format("socket连接异常：UnknownHostException：%s", e.getMessage()), e);
            } catch (IOException e) {
                throw new DtLoaderException(String.format("socket连接异常：%s", e.getMessage()), e);
            } finally {
                if (Objects.nonNull(socket) && socket.isClosed()){
                    try {
                        socket.close();
                    } catch (IOException e) {
                        log.error("关闭socket连接异常", e);
                    }
                }
            }
        }
        return true;
    }
}
