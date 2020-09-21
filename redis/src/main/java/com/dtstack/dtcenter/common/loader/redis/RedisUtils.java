package com.dtstack.dtcenter.common.loader.redis;

import com.dtstack.dtcenter.common.loader.common.utils.AddressUtil;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RedisSourceDTO;
import com.dtstack.dtcenter.loader.enums.RedisMode;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:38 2020/2/4
 * @Description：Redis 工具类
 */
@Slf4j
public class RedisUtils {
    private static Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>(.*)):((?<port>\\d+))*");
    private static final int DEFAULT_PORT = 6379;
    private static final int TIME_OUT = 5 * 1000;

    public static boolean checkConnection(ISourceDTO iSource) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) iSource;
        log.info("获取 Redis 数据源连接, host : {}, port : {}", redisSourceDTO.getMaster(), redisSourceDTO.getHostPort());
        RedisMode redisMode = redisSourceDTO.getRedisMode() != null ? redisSourceDTO.getRedisMode() : RedisMode.Standalone;
        switch (redisMode) {
            case Standalone:
                return checkConnectionStandalone(redisSourceDTO);
            case Sentinel:
                return checkRedisConnectionSentinel(redisSourceDTO);
            case Cluster:
                return checkRedisConnectionCluster(redisSourceDTO);
            default:
                throw new DtLoaderException("暂不支持的模式");
        }
    }

    private static boolean checkConnectionStandalone(ISourceDTO iSource) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) iSource;
        String password = redisSourceDTO.getPassword();
        String hostPort = redisSourceDTO.getHostPort();

        int db = StringUtils.isNotEmpty(redisSourceDTO.getSchema()) ? Integer.parseInt(redisSourceDTO.getSchema()) : 0;
        Matcher matcher = HOST_PORT_PATTERN.matcher(hostPort);

        Preconditions.checkArgument(matcher.find(), "hostPort格式异常");

        String host = matcher.group("host");
        String portStr = matcher.group("port");
        int port = portStr == null ? DEFAULT_PORT : Integer.parseInt(portStr);

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            if (StringUtils.isEmpty(password)) {
                pool = new JedisPool(new GenericObjectPoolConfig(), host, port, TIME_OUT);
            } else {
                pool = new JedisPool(new GenericObjectPoolConfig(), host, port, TIME_OUT, password, db);
            }
            jedis = pool.getResource();
            jedis.select(db);
            return true;
        } finally {
            if (jedis != null) {
                jedis.close();
            }

            if (pool != null) {
                pool.close();
            }
        }
    }

    public static boolean checkRedisConnectionCluster(ISourceDTO iSource) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) iSource;
        String hostPorts = redisSourceDTO.getHostPort();
        String password = redisSourceDTO.getPassword();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(2);
        poolConfig.setMaxIdle(2);
        poolConfig.setMaxWaitMillis(1000);
        JedisCluster cluster = null;

        Preconditions.checkArgument(StringUtils.isNotBlank(hostPorts), "hostPort不能为空");

        Set<HostAndPort> nodes = getHostAndPorts(hostPorts);

        Preconditions.checkArgument(CollectionUtils.isNotEmpty(nodes), "没有有效ip和端口");

        // redis集群模式不带密码，创建客户端不会主动去连接集群，所以需要用telnet检测
        for (HostAndPort node : nodes) {
            if (!AddressUtil.telnet(node.getHost(), node.getPort())) {
                return false;
            }
        }

        try {
            if (StringUtils.isNotBlank(password)) {
                cluster = new JedisCluster(nodes, 1000, 1000, 100, password, poolConfig);
            } else {
                cluster = new JedisCluster(nodes, poolConfig);
            }
            return true;
        } finally {
            try {
                if (cluster != null) {
                    cluster.close();
                }
            } catch (Exception e) {
                //
            }
        }
    }

    public static boolean checkRedisConnectionSentinel(ISourceDTO iSource) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) iSource;
        String masterName = redisSourceDTO.getMaster();
        String hostPorts = redisSourceDTO.getHostPort();
        int db = StringUtils.isEmpty(redisSourceDTO.getSchema()) ? 0 : Integer.parseInt(redisSourceDTO.getSchema());
        String password = redisSourceDTO.getPassword();
        Preconditions.checkArgument(StringUtils.isNotBlank(hostPorts), "hostPort不能为空");
        Set<HostAndPort> nodes = getHostAndPorts(hostPorts);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(nodes), "没有有效ip和端口");

        Set<String> sentinels = nodes.stream().map(hostAndPort -> hostAndPort.getHost() + ":" + hostAndPort.getPort())
                .collect(Collectors.toSet());

        JedisSentinelPool sentinelPool = null;
        Jedis jedis = null;
        try {
            if (StringUtils.isNotBlank(password)) {
                sentinelPool = new JedisSentinelPool(masterName, sentinels, password);
            } else {
                sentinelPool = new JedisSentinelPool(masterName, sentinels);
            }
            jedis = sentinelPool.getResource();
            jedis.select(db);
            return true;
        } finally {
            if (sentinelPool != null) {
                sentinelPool.close();
            }
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private static Set<HostAndPort> getHostAndPorts(String hostPorts) {
        Set<HostAndPort> nodes = new LinkedHashSet<>();
        String[] split = hostPorts.split(",");
        for (String node : split) {
            String[] nodeInfo = node.split(":");
            if (nodeInfo.length == 2) {
                nodes.add(new HostAndPort(nodeInfo[0].trim(), Integer.valueOf(nodeInfo[1].trim())));
            }
        }
        return nodes;
    }
}
