package com.dtstack.dtcenter.common.loader.redis;

import com.dtstack.dtcenter.common.loader.common.exception.IErrorPattern;
import com.dtstack.dtcenter.common.loader.common.service.ErrorAdapterImpl;
import com.dtstack.dtcenter.common.loader.common.service.IErrorAdapter;
import com.dtstack.dtcenter.common.loader.common.utils.AddressUtil;
import com.dtstack.dtcenter.loader.dto.RedisQueryDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RedisSourceDTO;
import com.dtstack.dtcenter.loader.enums.RedisCompareOp;
import com.dtstack.dtcenter.loader.enums.RedisDataType;
import com.dtstack.dtcenter.loader.enums.RedisMode;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
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
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
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

    private static final int LIMIT_MAX_KEY = 1000;

    private static final IErrorPattern ERROR_PATTERN = new RedisErrorPattern();

    // 异常适配器
    private static final IErrorAdapter ERROR_ADAPTER = new ErrorAdapterImpl();

    public static boolean checkConnection(ISourceDTO iSource) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) iSource;
        log.info("get Redis connected, host : {}, port : {}", redisSourceDTO.getMaster(), redisSourceDTO.getHostPort());
        RedisMode redisMode = redisSourceDTO.getRedisMode() != null ? redisSourceDTO.getRedisMode() : RedisMode.Standalone;
        try {
            switch (redisMode) {
                case Standalone:
                    return checkConnectionStandalone(redisSourceDTO);
                case Sentinel:
                    return checkRedisConnectionSentinel(redisSourceDTO);
                case Cluster:
                    return checkRedisConnectionCluster(redisSourceDTO);
                default:
                    throw new DtLoaderException("Unsupported mode");
            }
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            if (e instanceof JedisConnectionException && e.getCause() != null) {
                errorMsg = e.getCause().getMessage();
            }
            throw new DtLoaderException(ERROR_ADAPTER.connAdapter(errorMsg, ERROR_PATTERN), e);
        }
    }

    public static List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) source;
        String tableName = queryDTO.getTableName();
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("preview table name not empty");
        }
        log.info("get Redis connected, host : {}, port : {}", redisSourceDTO.getMaster(), redisSourceDTO.getHostPort());
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
            throw new DtLoaderException("get redis hash data exception");
        }
        for (int index = 0; index < values.size(); index++) {
            resultMap.put(fieldKey.get(index), values.get(index));
        }
        redisResult.add(resultMap);
        result.add(redisResult);
        return result;
    }

    public static Map<String, Object> executeQuery(ISourceDTO source, RedisQueryDTO queryDTO) {
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) source;
        AssertUtils.notNull(queryDTO.getKeys(), "keys not null");
        AssertUtils.notNull(queryDTO.getRedisDataType(), "redis data type not null");
        AssertUtils.notNull(queryDTO.getRedisCompareOp(), "operate not null");
        log.info("get Redis connected, host : {}, port : {}", redisSourceDTO.getMaster(), redisSourceDTO.getHostPort());
        return execute(redisSourceDTO, queryDTO);
    }

    private static Map<String, Object> execute(RedisSourceDTO redisSourceDTO, RedisQueryDTO queryDTO) {
        RedisMode redisMode = redisSourceDTO.getRedisMode() != null ? redisSourceDTO.getRedisMode() : RedisMode.Standalone;
        Pool<Jedis> redisPool = null;
        Jedis jedis = null;
        JedisCluster jedisCluster = null;
        try {
            if (RedisMode.Standalone.equals(redisMode) || RedisMode.Sentinel.equals(redisMode)) {
                redisPool = RedisMode.Standalone.equals(redisMode) ? getRedisPool(redisSourceDTO) : getSentinelPool(redisSourceDTO);
                jedis = redisPool.getResource();
                int db = StringUtils.isEmpty(redisSourceDTO.getSchema()) ? 0 : Integer.parseInt(redisSourceDTO.getSchema());
                jedis.select(db);
                return getRedisValues(jedis, queryDTO, jedis::scan);
            } else {
                jedisCluster = getRedisCluster(redisSourceDTO);
                return getRedisValues(jedisCluster, queryDTO, jedisCluster::scan);
            }
        } finally {
            // 关闭
            close(jedis, jedisCluster, redisPool);

        }
    }

    public static List<String> getRedisKeys(ISourceDTO source, RedisQueryDTO queryDTO) {
        AssertUtils.notNull(queryDTO.getRedisDataType(), "redis data type not null");
        RedisSourceDTO redisSourceDTO = (RedisSourceDTO) source;
        RedisMode redisMode = redisSourceDTO.getRedisMode() != null ? redisSourceDTO.getRedisMode() : RedisMode.Standalone;
        Pool<Jedis> redisPool = null;
        Jedis jedis = null;
        JedisCluster jedisCluster = null;
        try {
            if (RedisMode.Standalone.equals(redisMode) || RedisMode.Sentinel.equals(redisMode)) {
                redisPool = RedisMode.Standalone.equals(redisMode) ? getRedisPool(redisSourceDTO) : getSentinelPool(redisSourceDTO);
                jedis = redisPool.getResource();
                int db = StringUtils.isEmpty(redisSourceDTO.getSchema()) ? 0 : Integer.parseInt(redisSourceDTO.getSchema());
                jedis.select(db);
                return previewRedisKeys(jedis, queryDTO, jedis::scan);
            } else {
                jedisCluster = getRedisCluster(redisSourceDTO);
                return previewRedisKeys(jedisCluster, queryDTO, jedisCluster::scan);
            }
        } finally {
            // 关闭
            close(jedis, jedisCluster, redisPool);

        }
    }

    /**
     * 校验传入的key 类型是否一致
     * @param jedis
     * @param keys
     * @param dataType
     */
    private static void checkKeysType(JedisCommands jedis, List<String> keys, RedisDataType dataType) {
        for (String key : keys) {
            String type = jedis.type(key);
            AssertUtils.isTrue(dataType.name().equalsIgnoreCase(type), String.format("redis key :%s,expect type:%s, actual type: %s", key, dataType.name(), type));
        }
    }

    /**
     * 获取所有的key ,模糊查询
     * @param queryDTO
     * @param function
     * @return
     */
    public static List<String> getRedisKeys(JedisCommands jedis, RedisQueryDTO queryDTO, BiFunction<String, ScanParams, ScanResult<String>> function) {
        List<String> list = new ArrayList<>();
        //如果不是模糊查询，校验传入的key 类型是否一致
        if (!RedisCompareOp.LIKE.equals(queryDTO.getRedisCompareOp())) {
            checkKeysType(jedis, queryDTO.getKeys(), queryDTO.getRedisDataType());
            return queryDTO.getKeys();
        }
        String dataType = queryDTO.getRedisDataType().name();
        for (String key : queryDTO.getKeys()) {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParam = new ScanParams();
            scanParam.match(key);
            int count = 0;
            int keyLimit = queryDTO.getKeyLimit() != null && queryDTO.getKeyLimit() > 0 ? queryDTO.getKeyLimit() : LIMIT_MAX_KEY;
            do {
                ScanResult<String> scan = function.apply(cursor, scanParam);
                for (String scanKey : scan.getResult()) {
                    if (dataType.equalsIgnoreCase(jedis.type(scanKey))) {
                        list.add(scanKey);
                        if (++count >= keyLimit) {
                            return list;
                        }
                    }
                }
                cursor = scan.getStringCursor();
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        }
        list.sort(Comparator.naturalOrder());
        return list;
    }

    public static List<String> previewRedisKeys(JedisCommands jedis, RedisQueryDTO queryDTO, BiFunction<String, ScanParams, ScanResult<String>> function) {
        List<String> list = new ArrayList<>();
        String dataType = queryDTO.getRedisDataType().name();
        int keyLimit = queryDTO.getKeyLimit() != null && queryDTO.getKeyLimit() > 0 ? queryDTO.getKeyLimit() : LIMIT_MAX_KEY;
        if (queryDTO.getKeys() != null) {
            for (String key : queryDTO.getKeys()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParam = new ScanParams();
                scanParam.match(key);
                int count = 0;
                do {
                    ScanResult<String> scan = function.apply(cursor, scanParam);
                    for (String scanKey : scan.getResult()) {
                        if (dataType.equalsIgnoreCase(jedis.type(scanKey))) {
                            list.add(scanKey);
                            if (++count >= keyLimit) {
                                return list;
                            }
                        }
                    }
                    cursor = scan.getStringCursor();
                } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
            }
        } else {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParam = new ScanParams();
            int count = 0;
            do {
                ScanResult<String> scan = function.apply(cursor, scanParam);
                for (String scanKey : scan.getResult()) {
                    if (dataType.equalsIgnoreCase(jedis.type(scanKey))) {
                        list.add(scanKey);
                        if (++count >= keyLimit) {
                            return list;
                        }
                    }
                }
                cursor = scan.getStringCursor();
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        }
        list.sort(Comparator.naturalOrder());
        return list;
    }




    private static Map<String, Object> getRedisValues(JedisCommands jedis, RedisQueryDTO queryDTO, BiFunction<String, ScanParams, ScanResult<String>> function) {

        List<String> keys = getRedisKeys(jedis, queryDTO, function);

        RedisDataType dataType = queryDTO.getRedisDataType();
        Map<String, Object> result;
        Integer limit = queryDTO.getResultLimit() != null && queryDTO.getResultLimit() > 0 ? queryDTO.getResultLimit() - 1 : -1;
        switch (dataType) {
            case STRING:
                result = getStringRedisValue(jedis, keys);
                break;
            case HASH:
                result = getHashRedisValue(jedis, keys);
                break;
            case LIST:
                result = getListRedisValue(jedis, keys, limit);
                break;
            case SET:
                result = getSetRedisValue(jedis, keys);
                break;
            case ZSET:
                result = getSortSetRedisValue(jedis, keys, limit);
                break;
            default:
                throw new DtLoaderException("Unsupported dataType");
        }
        return result;
    }


    /**
     * 查询 string 类型
     * @param jedis
     * @param keys
     * @return
     */
    private static Map<String, Object> getStringRedisValue(JedisCommands jedis, List<String> keys) {
        Map<String, Object> resultMap = new HashMap<>();
        if (CollectionUtils.isEmpty(keys)) {
            return resultMap;
        }
        for (String key : keys) {
            String result = jedis.get(key);
            resultMap.put(key, result);
        }
        return resultMap;
    }

    /**
     * 查询list类型
     * @param jedis
     * @param keys
     * @return
     */
    private static Map<String, Object> getListRedisValue(JedisCommands jedis, List<String> keys, Integer limit) {
        Map<String, Object> resultMap = new HashMap<>();
        if (CollectionUtils.isEmpty(keys)) {
            return resultMap;
        }
        for (String key : keys) {
            List<String> result = jedis.lrange(key, 0, limit);
            result = CollectionUtils.isEmpty(result) ? new ArrayList<>() : result;
            resultMap.put(key, result);
        }
        return resultMap;
    }

    /**
     * 查询set 类型， smembers
     * @param jedis
     * @param keys
     * @return
     */
    private static Map<String, Object> getSetRedisValue(JedisCommands jedis, List<String> keys) {
        Map<String, Object> resultMap = new HashMap<>();
        if (CollectionUtils.isEmpty(keys)) {
            return resultMap;
        }
        ScanParams scanParams = new ScanParams();
        for (String key : keys) {
            String cursor = ScanParams.SCAN_POINTER_START;
            List<String> list = new ArrayList<>();
            do {
                ScanResult<String> scanResult = jedis.sscan(key, cursor, scanParams);
                List<String> result = scanResult.getResult();
                list.addAll(result);
                cursor = scanResult.getStringCursor();
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
            resultMap.put(key, list);
        }
        return resultMap;
    }

    /**
     * 查询zset 类型，zrange
     * @param jedis
     * @param keys
     * @return
     */
    private static Map<String, Object> getSortSetRedisValue(JedisCommands jedis, List<String> keys, Integer limit) {
        Map<String, Object> resultMap = new HashMap<>();
        if (CollectionUtils.isEmpty(keys)) {
            return resultMap;
        }
        for (String key : keys) {
            Set<String> result = jedis.zrange(key, 0, limit);
            result = CollectionUtils.isEmpty(result) ? new HashSet<>() : result;
            resultMap.put(key, result);
        }
        return resultMap;
    }


    /**
     * 查询hash 类型的表
     * @param jedis
     * @param keys
     * @return
     */
    private static Map<String, Object> getHashRedisValue(JedisCommands jedis, List<String> keys) {
        Map<String, Object> resultMap = new HashMap<>();
        if (CollectionUtils.isEmpty(keys)) {
            return resultMap;
        }
        ScanParams scanParams = new ScanParams();
        for (String key : keys) {
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan(key, cursor, scanParams);
                List<Map.Entry<String, String>> tuples = scanResult.getResult();
                List<String> list = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(tuples)) {
                    list = tuples.stream().map(Map.Entry::getKey).collect(Collectors.toList());
                }
                resultMap.put(key, list);
                cursor = scanResult.getStringCursor();
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        }
        return resultMap;
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
        Preconditions.checkArgument(StringUtils.isNotBlank(hostPorts), "hostPort not empty");
        Set<HostAndPort> nodes = getHostAndPorts(hostPorts);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(nodes), "invalid ip and port");

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

        Preconditions.checkArgument(matcher.find(), "hostPort Format exception");

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
        Preconditions.checkArgument(StringUtils.isNotBlank(hostPorts), "hostPort not empty");

        Set<HostAndPort> nodes = getHostAndPorts(hostPorts);

        Preconditions.checkArgument(CollectionUtils.isNotEmpty(nodes), "invalid ip and port");

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
            Matcher matcher = HOST_PORT_PATTERN.matcher(node);
            if (matcher.find()) {
                String host = matcher.group("host");
                String portStr = matcher.group("port");
                if (StringUtils.isNotBlank(host) && StringUtils.isNotBlank(portStr)) {
                    // 转化为int格式的端口
                    int port = Integer.parseInt(portStr);
                    nodes.add(new HostAndPort(host, port));
                }
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
            throw new DtLoaderException(String.format("redis close resource error,%s", e.getMessage()), e);
        }
    }
}
