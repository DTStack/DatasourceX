package com.dtstack.dtcenter.common.loader.phoenix;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.Phoenix5SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:32 2020/7/8
 * @Description：默认 Phoenix 连接工厂
 */
@Slf4j
public class PhoenixConnFactory extends ConnFactory {

    /**
     * 获取phoenix5连接超时时间。单位：秒
      */
    private final static int CONN_TIMEOUT = 60;

    public PhoenixConnFactory() {
        this.driverName = DataBaseType.Phoenix.getDriverClassName();
        this.testSql = DataBaseType.Phoenix.getTestSql();
    }

    /**
     * 获取phoenix5连接，由于jdbc不支持设置超时时间，通过线程池来实现
     *
     * @param source
     * @return
     * @throws Exception
     */
    @Override
    public Connection getConn(ISourceDTO source) throws Exception {
        init();
        Phoenix5SourceDTO phoenix5SourceDTO = (Phoenix5SourceDTO) source;
        Connection conn;
        Future<Connection> future = null;
        try {
            // Phoenix不支持直接设置连接超时，所以这里使用线程池的方式来控制数据库连接超时
            Callable<Connection> call = () -> PhoenixConnFactory.super.getConn(phoenix5SourceDTO);
            future = executor.submit(call);
            // 如果在设定超时(以秒为单位)之内，还没得到 Connection 对象，则认为连接超时，不继续阻塞
            conn = future.get(CONN_TIMEOUT, TimeUnit.SECONDS);
            if (Objects.isNull(conn)) {
                throw new DtLoaderException("获取phoenix5连接失败！");
            }
        } catch (InterruptedException e) {
            log.error("获取连接线程中断！url=" + phoenix5SourceDTO.getUrl(), e);
            throw new DtLoaderException("获取phoenix5过程中线程中中断！", e);
        } catch (ExecutionException e) {
            log.error("获取连接出错！url=" + phoenix5SourceDTO.getUrl(), e);
            throw new DtLoaderException("获取phoenix5连接出错！", e);
        } catch (TimeoutException e) {
            log.error("获取连接超时！url=" + phoenix5SourceDTO.getUrl(), e);
            throw new DtLoaderException("获取phoenix5连接超时！", e);
        } finally {
            if (Objects.nonNull(future)) {
                future.cancel(true);
            }
        }
        return conn;
    }
}
