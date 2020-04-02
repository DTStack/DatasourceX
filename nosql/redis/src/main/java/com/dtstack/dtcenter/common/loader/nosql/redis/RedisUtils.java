package com.dtstack.dtcenter.common.loader.nosql.redis;

import com.dtstack.dtcenter.common.util.AddressUtil;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:38 2020/2/4
 * @Description：Redis 工具类
 */
@Slf4j
public class RedisUtils {
    private static Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>(.*)):((?<port>\\d+))*");

    public static final int DEFAULT_PORT = 6379;

    public static final String DEFAULT_HOST = "localhost";

    public static final int TIME_OUT = 5 * 1000;

    public static boolean checkConnection(SourceDTO source) {
        int db = StringUtils.isBlank(source.getSchema()) ? 0 : Integer.valueOf(source.getSchema());

        return checkConnection(source.getUrl(), Integer.valueOf(source.getHostPort()), source.getPassword(), db);
    }

    private static boolean checkConnection(String host, int port, String password, int db) {
        boolean check = false;
        JedisPool pool = null;
        Jedis jedis = null;
        try {
            if (!AddressUtil.telnet(host, port)) {
                return false;
            }
            if (StringUtils.isEmpty(password)) {
                pool = new JedisPool(new GenericObjectPoolConfig(), host, port, TIME_OUT);
            } else {
                pool = new JedisPool(new GenericObjectPoolConfig(), host, port, TIME_OUT, password, db);
            }
            jedis = pool.getResource();
            jedis.select(db);

            check = true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }

            if (pool != null) {
                pool.close();
            }
        }

        return check;
    }
}
