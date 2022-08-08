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

package com.dtstack.source.loader.server.proxy;

import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.cache.client.RealClientCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cglib.proxy.Proxy;

import java.lang.reflect.Method;
import java.util.Objects;

/**
 * server 上层接口具体实现接口代理对象
 *
 * @author ：wangchuan
 * date：Created in 下午9:20 2021/12/20
 * company: www.dtstack.com
 */
@Slf4j
@SuppressWarnings("unchecked")
public class ServerImplProxy<T> implements FactoryBean<T> {

    protected final Class<T> interfaceClass;

    public ServerImplProxy(Class<T> interfaceClass) {
        this.interfaceClass = interfaceClass;
    }

    @Override
    public T getObject() {
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(),
                new Class[]{interfaceClass}, this::proxyRun);
    }

    @Override
    public Class<?> getObjectType() {
        return interfaceClass;
    }

    protected Object proxyRun(Object proxy, Method method, Object[] args) throws Throwable {
        if (checkObjMethod(method)) {
            // Object 的方法执行
            log.info("Execute object method: {}", method.getName());
            return method.invoke(interfaceClass, args);
        }
        log.info("Execute Proxy method: {}", method.getName());

        if (ArrayUtils.isEmpty(args)) {
            throw new DtLoaderException("parameter can't be null");
        }

        ISourceDTO sourceDTO = null;
        for (Object arg : args) {
            if (arg instanceof ISourceDTO) {
                sourceDTO = (ISourceDTO) arg;
                break;
            }
        }
        if (Objects.isNull(sourceDTO)) {
            throw new DtLoaderException("The parameter does not contain an instance of ISourceDTO");
        }

        return method.invoke(RealClientCache.getByClass(interfaceClass, sourceDTO.getSourceType()), args);
    }

    /**
     * 校验是否是 Object 方法
     *
     * @param method 方法
     * @return 是否是 Object 方法
     */
    private boolean checkObjMethod(Method method) {
        Method[] methods = Object.class.getMethods();
        for (Method objectMethod : methods) {
            if (objectMethod.equals(method)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }
}
