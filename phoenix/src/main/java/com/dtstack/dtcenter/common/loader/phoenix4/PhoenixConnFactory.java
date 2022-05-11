/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.common.loader.phoenix4;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.PhoenixSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:51 2020/2/27
 * @Description：默认 Phoenix 连接工厂
 */
@Slf4j
public class PhoenixConnFactory extends ConnFactory {

    /**
     * 获取phoenix连接超时时间。单位：秒
     */
    private final static int CONN_TIMEOUT = 60;

    public PhoenixConnFactory() {
        this.driverName = DataBaseType.Phoenix.getDriverClassName();
        this.testSql = DataBaseType.Phoenix.getTestSql();
        this.errorPattern = new PhoenixErrorPattern();
    }

    /**
     * 获取phoenix连接，由于jdbc不支持设置超时时间，通过线程池来实现
     *
     * @param source
     * @return
     * @throws Exception
     */
    @Override
    public Connection getConn(ISourceDTO source, String taskParams) throws Exception {
        init();
        PhoenixSourceDTO phoenixSourceDTO = (PhoenixSourceDTO) source;
        Connection conn;
        Future<Connection> future = null;
        try {
            // Phoenix不支持直接设置连接超时，所以这里使用线程池的方式来控制数据库连接超时
            Callable<Connection> call = () -> PhoenixConnFactory.super.getConn(phoenixSourceDTO, taskParams);
            future = executor.submit(call);
            // 如果在设定超时(以秒为单位)之内，还没得到 Connection 对象，则认为连接超时，不继续阻塞
            conn = future.get(CONN_TIMEOUT, TimeUnit.SECONDS);
            if (Objects.isNull(conn)) {
                throw new DtLoaderException("get phoenix connection error！");
            }
        }  catch (TimeoutException e) {
            throw new DtLoaderException(String.format("get phoenix connection timeout,%s", e.getMessage()), e);
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            if (e.getCause() != null && e.getCause() instanceof DtLoaderException) {
                errorMsg = e.getCause().getMessage();
            }
            // 对异常进行统一处理
            throw new DtLoaderException(errorAdapter.connAdapter(errorMsg, errorPattern), e);
        } finally {
            if (Objects.nonNull(future)) {
                future.cancel(true);
            }
        }
        return conn;
    }
}
