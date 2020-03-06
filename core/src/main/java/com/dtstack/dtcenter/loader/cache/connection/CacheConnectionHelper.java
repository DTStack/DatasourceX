package com.dtstack.dtcenter.loader.cache.connection;

import com.dtstack.dtcenter.common.Callback;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:50 2020/3/4
 * @Description：缓存连接池配置 使用步骤：
 * 1. 先使用 start 开启缓存连接池配置，如果有要求多次请求复用，请传入唯一 sessionKey
 * 2. 使用完请使用 stop 关闭缓存，如果使用了 start 则必须使用 stop 关闭线程池，否则会存储到缓存池中，轮询超时销毁
 * 3. 如果 VertX 等服务有线程池的情况，需要再请求技术之后，不管有没有 stop 都需要一次 removeCacheConnection()
 */
@Slf4j
public class CacheConnectionHelper {
    private static Random random = new Random();

    /**
     * 线程级连接信息 用于处理单次单线程隔离 connection，每个线程都初始化
     */
    private static ThreadLocal<DataSourceConnection> LOCAL_CON =
            ThreadLocal.withInitial(() -> new DataSourceConnection());

    /**
     * 判断是否开启缓存
     */
    private static ThreadLocal<Boolean> START_CACHE = ThreadLocal.withInitial(() -> Boolean.FALSE);

    /**
     * 更新缓存唯一键，之前的连接如果存在，直接关闭
     *
     * @param sessionKey
     */
    public static void updateSessionKey(String sessionKey) {
        LOCAL_CON.get().updateSessionKey(sessionKey);
    }

    /**
     * 获取缓存唯一键
     *
     * @return
     */
    public static String getSessionKey() {
        return LOCAL_CON.get().getSessionKey();
    }

    /**
     * 去除缓存配置
     */
    public static void removeCacheConnection() {
        LOCAL_CON.remove();
        START_CACHE.remove();
    }

    public static void closeCacheConnection() {
        if (StringUtils.isBlank(getSessionKey())) {
            log.info("当前不存在缓存连接，无需关闭：" + Thread.currentThread().getName());
            removeCacheConnection();
            return;
        }
        closeCacheConnection(getSessionKey());
    }

    /**
     * 关闭特定 KEY 的缓存连接
     *
     * @param sessionKey
     */
    public static void closeCacheConnection(String sessionKey) {
        if (StringUtils.isBlank(sessionKey)) {
            closeCacheConnection();
            return;
        }

        removeCacheConnection();
        HashCacheConnectionKey.clearKey(sessionKey, null, false);
    }

    /**
     * 判断是否开启缓存
     *
     * @return
     */
    public static Boolean isStart() {
        return START_CACHE.get();
    }

    public static String startCacheConnection() {
        // 生成随机唯一键，不允许往下面方法传空
        String sessionKey = "session_key_" + random.nextInt(100000000) + "_" + System.currentTimeMillis();
        return startCacheConnection(sessionKey);
    }

    /**
     * 开启缓存连接池
     * 1. 设置线程级的缓存唯一键
     * 2. 初始化缓存连接池
     *
     * @param sessionKey
     * @return
     */
    public static String startCacheConnection(String sessionKey) {
        if (StringUtils.isBlank(sessionKey)) {
            return startCacheConnection();
        }
        log.info("start connection ,sessionKey = {}", sessionKey);
        // 稳一手，不管第三方是否使用缓存，重置一次并开启缓存标志
        LOCAL_CON.remove();
        START_CACHE.set(Boolean.TRUE);

        if (HashCacheConnectionKey.isContainSessionKey(sessionKey)) {
            LOCAL_CON.set(HashCacheConnectionKey.getSourceConnection(sessionKey));
            return sessionKey;
        }

        // 初始化缓存连接池
        LOCAL_CON.get().updateSessionKey(sessionKey);
        HashCacheConnectionKey.addKey(sessionKey, LOCAL_CON.get());
        return sessionKey;
    }

    /**
     * 获取 缓存连接
     *
     * @return
     */
    @Nullable
    public static Connection getConnection(Integer sourceType) {
        if (StringUtils.isBlank(getSessionKey())) {
            return null;
        }

        return getConnection(getSessionKey(), sourceType);
    }

    public static Connection getConnection(String sessionKey, Integer sourceType) {
        if (StringUtils.isBlank(sessionKey)) {
            return getConnection(sourceType);
        }

        // 判断 SessionKey 是否相同，相同则直接返回这个缓存连接中的
        if (sessionKey.equals(getSessionKey())) {
            return LOCAL_CON.get().getConnection(sourceType);
        }

        // 获取 HashKey 中历史的缓存信息，获取其中的连接
        DataSourceConnection sourceConnection = HashCacheConnectionKey.getSourceConnection(sessionKey);
        if (sourceConnection == null) {
            return null;
        }

        return sourceConnection.getConnection(sourceType);
    }

    @Nullable
    public static Connection getConnection(Integer sourceType, Callback<Connection> callback) throws SQLException {
        if (StringUtils.isBlank(getSessionKey())) {
            return null;
        }

        return getConnection(getSessionKey(), sourceType, callback);
    }

    public static Connection getConnection(String sessionKey, Integer sourceType, Callback<Connection> callback) throws SQLException {
        Connection connection = getConnection(sessionKey, sourceType);

        // 如果在当前所有的缓存池中存在，则直接获取
        if (connection != null) {
            return connection;
        }

        // 如果不存在则先获取再存储进去
        return setConnection(sessionKey, sourceType, (Connection) callback.submit(null));
    }

    /**
     * 设置缓存连接池信息
     *
     * @param sessionKey
     * @param sourceType
     * @param connection
     * @return
     * @throws SQLException
     */
    private static Connection setConnection(String sessionKey, Integer sourceType, Connection connection) throws SQLException {
        DataSourceConnection sourceConnection = HashCacheConnectionKey.getSourceConnection(sessionKey);
        if (sourceConnection == null) {
            return connection;
        }

        sourceConnection.addNode(sourceType, connection);
        return connection;
    }
}
