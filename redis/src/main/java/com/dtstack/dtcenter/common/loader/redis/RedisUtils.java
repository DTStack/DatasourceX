package com.dtstack.dtcenter.common.loader.redis;

import com.dtstack.dtcenter.common.loader.common.utils.AddressUtil;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RedisSourceDTO;
import com.dtstack.dtcenter.loader.enums.RedisMode;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import redis.clients.util.Pool;

import java.io.Closeable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    public static List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) source;
        String tableName = queryDTO.getTableName();
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("预览数据表名不能为空");
        }
        log.info("获取 Redis 数据源连接, host : {}, port : {}", redisSourceDTO.getMaster(), redisSourceDTO.getHostPort());
        RedisMode redisMode = redisSourceDTO.getRedisMode() != null ? redisSourceDTO.getRedisMode() : RedisMode.Standalone;
        List<List<Object>> result = Lists.newArrayList();

        List<Object> redisResult = Lists.newArrayList();
        Map<String, Object> resultMap = Maps.newHashMap();

        List<String> fieldKey;
        List<String> values ;
        if (RedisMode.Standalone.equals(redisMode) || RedisMode.Sentinel.equals(redisMode)) {
            Pool<Jedis> redisPool = null;
            Jedis jedis = null;
            try {
                if (RedisMode.Standalone.equals(redisMode)) {
                    redisPool = getRedisPool(source);
                } else {
                    redisPool = getSentinelPool(source);
                }
                jedis = redisPool.getResource();
                int db = StringUtils.isEmpty(redisSourceDTO.getSchema()) ? 0 : Integer.parseInt(redisSourceDTO.getSchema());
                jedis.select(db);
                Set<String> keys = jedis.hkeys(tableName);
                if (CollectionUtils.isEmpty(keys)) {
                    return result;
                }
                fieldKey = keys.stream().limit(queryDTO.getPreviewNum()).collect(Collectors.toList());
                values = jedis.hmget(tableName, fieldKey.toArray(new String[]{}));
            } finally {
                // 关闭资源
                close(jedis, redisPool);
            }
        }  else {
            JedisCluster redisCluster = null;
            try {
                redisCluster = getRedisCluster(source);
                Set<String> keys = redisCluster.hkeys(tableName);
                if (CollectionUtils.isEmpty(keys)) {
                    return result;
                }
                fieldKey = keys.stream().limit(queryDTO.getPreviewNum()).collect(Collectors.toList());
                values = redisCluster.hmget(tableName, fieldKey.toArray(new String[]{}));
            } finally {
                // 关闭资源
                close(redisCluster);
            }
        }

        if (fieldKey.size() != values.size()) {
            throw new DtLoaderException("获取redis hash数据异常");
        }
        for (int index = 0; index < values.size(); index++) {
            resultMap.put(fieldKey.get(index), values.get(index));
        }
        redisResult.add(resultMap);
        result.add(redisResult);
        return result;
    }

    private static boolean checkConnectionStandalone(ISourceDTO source) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) source;
        JedisPool redisPool = null;
        Jedis jedis = null;
        int db = StringUtils.isNotEmpty(redisSourceDTO.getSchema()) ? Integer.parseInt(redisSourceDTO.getSchema()) : 0;
        try {
            redisPool = getRedisPool(source);
            jedis = redisPool.getResource();
            jedis.select(db);
            return true;
        } finally {
            // 关闭资源
            close(jedis, redisPool);
        }
    }

    public static boolean checkRedisConnectionCluster(ISourceDTO source) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) source;
        JedisCluster redisCluster = null;
        try {
            redisCluster = getRedisCluster(source);
            Set<HostAndPort> nodes = getHostAndPorts(redisSourceDTO.getHostPort());
            // redis集群模式不带密码，创建客户端不会主动去连接集群，所以需要用telnet检测
            for (HostAndPort node : nodes) {
                if (!AddressUtil.telnet(node.getHost(), node.getPort())) {
                    return false;
                }
            }
            return true;
        } finally {
            close(redisCluster);
        }
    }

    public static boolean checkRedisConnectionSentinel(ISourceDTO source) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) source;
        int db = StringUtils.isEmpty(redisSourceDTO.getSchema()) ? 0 : Integer.parseInt(redisSourceDTO.getSchema());
        JedisSentinelPool sentinelPool = null;
        Jedis jedis = null;
        try {
            sentinelPool = getSentinelPool(source);
            jedis = sentinelPool.getResource();
            jedis.select(db);
            return true;
        } finally {
            // 关闭资源
            close(jedis, sentinelPool);
        }
    }

    /**
     * 获取redis 哨兵模式连接池
     * @param source 数据源信息
     * @return redis 连接池
     */
    private static JedisSentinelPool getSentinelPool (ISourceDTO source) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) source;
        String masterName = redisSourceDTO.getMaster();
        String hostPorts = redisSourceDTO.getHostPort();
        String password = redisSourceDTO.getPassword();
        Preconditions.checkArgument(StringUtils.isNotBlank(hostPorts), "hostPort不能为空");
        Set<HostAndPort> nodes = getHostAndPorts(hostPorts);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(nodes), "没有有效ip和端口");

        Set<String> sentinels = nodes.stream().map(hostAndPort -> hostAndPort.getHost() + ":" + hostAndPort.getPort())
                .collect(Collectors.toSet());

        JedisSentinelPool sentinelPool;
        if (StringUtils.isNotBlank(password)) {
            sentinelPool = new JedisSentinelPool(masterName, sentinels, password);
        } else {
            sentinelPool = new JedisSentinelPool(masterName, sentinels);
        }
        return sentinelPool;
    }

    /**
     * 获取redis 单机模式连接池
     * @param source 数据源信息
     * @return redis 连接池
     */
    private static JedisPool getRedisPool (ISourceDTO source) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) source;
        String password = redisSourceDTO.getPassword();
        String hostPort = redisSourceDTO.getHostPort();
        int db = StringUtils.isNotEmpty(redisSourceDTO.getSchema()) ? Integer.parseInt(redisSourceDTO.getSchema()) : 0;
        Matcher matcher = HOST_PORT_PATTERN.matcher(hostPort);

        Preconditions.checkArgument(matcher.find(), "hostPort格式异常");

        String host = matcher.group("host");
        String portStr = matcher.group("port");
        int port = portStr == null ? DEFAULT_PORT : Integer.parseInt(portStr);
        JedisPool pool;
        if (StringUtils.isEmpty(password)) {
            pool = new JedisPool(new GenericObjectPoolConfig(), host, port, TIME_OUT);
        } else {
            pool = new JedisPool(new GenericObjectPoolConfig(), host, port, TIME_OUT, password, db);
        }
        return pool;
    }

    /**
     * 获取redis 集群模式 客户端
     * @param source 数据源信息
     * @return redis客户端
     */
    private static JedisCluster getRedisCluster (ISourceDTO source) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) source;
        String hostPorts = redisSourceDTO.getHostPort();
        String password = redisSourceDTO.getPassword();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(2);
        poolConfig.setMaxIdle(2);
        poolConfig.setMaxWaitMillis(1000);
        JedisCluster cluster;
        Preconditions.checkArgument(StringUtils.isNotBlank(hostPorts), "hostPort不能为空");

        Set<HostAndPort> nodes = getHostAndPorts(hostPorts);

        Preconditions.checkArgument(CollectionUtils.isNotEmpty(nodes), "没有有效ip和端口");

        if (StringUtils.isNotBlank(password)) {
            cluster = new JedisCluster(nodes, 1000, 1000, 100, password, poolConfig);
        } else {
            cluster = new JedisCluster(nodes, poolConfig);
        }
        return cluster;
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

    private static void close(Closeable... closeables) {
        try {
            if (Objects.nonNull(closeables)) {
                for (Closeable closeable : closeables) {
                    if (Objects.nonNull(closeable)) {
                        closeable.close();
                    }
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException("redis close resource error", e);
        }
    }
}
