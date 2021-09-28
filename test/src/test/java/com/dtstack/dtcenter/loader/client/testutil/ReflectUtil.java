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

package com.dtstack.dtcenter.loader.client.testutil;

import java.lang.reflect.Method;

/**
 * 反射工具类
 *
 * @author ：wangchuan
 * date：Created in 上午10:56 2021/6/4
 * company: www.dtstack.com
 */
public class ReflectUtil {

    @SuppressWarnings("unchecked")
    public static <T> T invokeMethod(Object obj, Class<T> returnType , String methodName, Class<?> [] parameterType, Object... args) {
        try {
            Method method = obj.getClass().getDeclaredMethod(methodName, parameterType);
            method.setAccessible(true);
            return (T) method.invoke(obj, args);
        } catch (Exception e) {
            throw new RuntimeException(String.format("invoke method error: %s", e.getMessage()), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T invokeStaticMethod(Class<?> source, Class<T> returnType , String methodName, Class<?> [] parameterType, Object... args) {
        try {
            Method method = source.getDeclaredMethod(methodName, parameterType);
            method.setAccessible(true);
            return (T) method.invoke(source.newInstance(), args);
        } catch (Exception e) {
            throw new RuntimeException(String.format("invoke method error: %s", e.getMessage()), e);
        }
    }
}
