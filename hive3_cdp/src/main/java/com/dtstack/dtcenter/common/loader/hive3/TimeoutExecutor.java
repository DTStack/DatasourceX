package com.dtstack.dtcenter.common.loader.hive3;

import com.dtstack.dtcenter.common.loader.common.DtClassThreadFactory;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>异步执行方法，用于设置方法执行超时时间
 *
 * @company: www.dtstack.com
 * @Author ：qianyi
 * @Date ：Created in 14:03 2021/05/13
 */
@Slf4j
public class TimeoutExecutor {

    // 异步方法执行线程池
    private static final ThreadPoolExecutor EXEC_TIMEOUT_POOL = new ThreadPoolExecutor(10, 10, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(1000), new DtClassThreadFactory("ExecTimeoutTask"));

    // 方法执行超时时间
    private final static int EXEC_TIMEOUT = 2 * 60;

    /**
     * 异步执行方法，超时时间默认两分钟
     *
     * @param exec 执行方法
     * @param <T>  范型
     * @return 返回方法执行结果
     */
    public static <T> T execAsync(Callable<T> exec) {
        return execAsync(exec, EXEC_TIMEOUT);
    }

    /**
     * 异步执行方法，可以设置超时时间
     *
     * @param exec    执行方法
     * @param timeout 超时时间
     * @param <T>     范型
     * @return 返回方法执行结果
     */
    public static <T> T execAsync(Callable<T> exec, Integer timeout) {
        timeout = Objects.isNull(timeout) ? EXEC_TIMEOUT : timeout;
        T result;
        Future<T> future = null;
        try {
            future = EXEC_TIMEOUT_POOL.submit(exec);
            result = future.get(timeout, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new DtLoaderException(String.format("method executed timeout !,%s", e.getMessage()), e);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("method executed error,%s", e.getMessage()), e);
        } finally {
            if (Objects.nonNull(future)) {
                future.cancel(true);
            }
        }
        return result;
    }

}
