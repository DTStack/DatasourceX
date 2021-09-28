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

package com.dtstack.dtcenter.loader.client.common;


import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

/**
 * 扫描所有dto覆盖测试
 */
public class DTOUtilTest {
    //要测试的包
    private static final String DTO_PACKAGE_NAME = "com.dtstack.dtcenter.loader.dto";
    private static final String CLIENT_PACKAGE_NAME = "com.dtstack.dtcenter.loader.cache";

    // 需要过滤的一些特殊的类方法
    private static final List<String> filterClazzMethodList = new ArrayList<String>();
    // 需要过滤的一些特殊的类属性
    private static final List<String> filterClazzFieldList = new ArrayList<String>();
    // 过滤一些特殊类--在类加载阶段就会过滤
    private static final List<String> filterClazzList = new ArrayList<String>();

    static {
        filterClazzFieldList.add("");
        // ==============================================================================分割线

        filterClazzMethodList.add("");

        // ================================================================================分割线
        filterClazzList.add("");
    }

    @Test
    public void dtoTest() {
        List<Class<?>> dtoClass = getClasses(DTO_PACKAGE_NAME);
        dtoTest(dtoClass);
        List<Class<?>> clientClass = getClasses(CLIENT_PACKAGE_NAME);
        dtoTest(clientClass);
    }

    private void dtoTest(List<Class<?>> allClass) {
        if (null != allClass) {
            for (Class classes : allClass) {// 循环反射执行所有类
                try {
                    boolean isAbstract = Modifier.isAbstract(classes.getModifiers());
                    if (classes.isInterface() || isAbstract) {// 如果是接口或抽象类,跳过
                        continue;
                    }
                    Constructor[] constructorArr = classes.getConstructors();
                    Object clazzObj = newConstructor(constructorArr, classes);
                    fieldTest(classes, clazzObj);

                    methodInvoke(classes, clazzObj);
                } catch (IllegalAccessException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InstantiationException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }


    private void fieldTest(Class<?> classes, Object clazzObj)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException {
        if (null == clazzObj) {
            return;
        }
        String s = clazzObj.toString();
        String clazzName = classes.getName();
        Field[] fields = classes.getDeclaredFields();
        if (null != fields && fields.length > 0) {
            for (Field field : fields) {
                String fieldName = field.getName();
                if (filterClazzFieldList.contains(clazzName + "." + fieldName)) {
                    continue;
                }
                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }
                Object fieldGetObj = field.get(clazzObj);
                if (!Modifier.isFinal(field.getModifiers()) || null == fieldGetObj) {
                    field.set(clazzObj, adaptorGenObj(field.getType()));
                }
            }
        }
    }

    /**
     * 功能描述: 执行方法<br>
     * 〈功能详细描述〉
     *
     * @param classes
     * @param clazzObj
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    private void methodInvoke(Class<?> classes, Object clazzObj)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, InstantiationException, DtLoaderException {
        String clazzName = classes.getName();
        Method[] methods = classes.getDeclaredMethods();
        if (null != methods && methods.length > 0) {
            for (Method method : methods) {
                String methodName = method.getName();
                String clazzMethodName = clazzName + "." + methodName;
                // 排除无法处理方法
                if (filterClazzMethodList.contains(clazzMethodName)) {
                    continue;
                }
                // 无论如何，先把权限放开
                method.setAccessible(true);
                Class<?>[] paramClassArrs = method.getParameterTypes();

                // 执行getset方法
                if (methodName.startsWith("set") && null != clazzObj) {
                    methodInvokeGetSet(classes, clazzObj, method, paramClassArrs, clazzMethodName, methodName);
                    continue;
                }
                // 如果是静态方法
                if (Modifier.isStatic(method.getModifiers()) && !classes.isEnum()) {
                    if (null == paramClassArrs || paramClassArrs.length == 0) {
                        method.invoke(null, null);
                    } else if (paramClassArrs.length == 1) {
                        System.out.println("clazzMethodName:" + clazzMethodName + "," + classes.isEnum());
                        method.invoke(null, adaptorGenObj(paramClassArrs[0]));
                    } else if (paramClassArrs.length == 2) {
                        method.invoke(null, adaptorGenObj(paramClassArrs[0]), adaptorGenObj(paramClassArrs[1]));
                    } else if (paramClassArrs.length == 3) {
                        method.invoke(null, adaptorGenObj(paramClassArrs[0]), adaptorGenObj(paramClassArrs[1]),
                                adaptorGenObj(paramClassArrs[2]));
                    } else if (paramClassArrs.length == 4) {
                        method.invoke(null, adaptorGenObj(paramClassArrs[0]), adaptorGenObj(paramClassArrs[1]),
                                adaptorGenObj(paramClassArrs[2]), adaptorGenObj(paramClassArrs[3]));
                    }
                    continue;
                }
                if (null == clazzObj) {
                    continue;
                }
                // 如果方法是toString,直接执行
                if ("toString".equals(methodName)) {
                    try {
                        Method toStringMethod = classes.getDeclaredMethod(methodName, null);
                        toStringMethod.invoke(clazzObj, null);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                // 其他方法
                if (null == paramClassArrs || paramClassArrs.length == 0) {
                    method.invoke(clazzObj, null);
                } else if (paramClassArrs.length == 1) {
                    method.invoke(clazzObj, adaptorGenObj(paramClassArrs[0]));
                } else if (paramClassArrs.length == 2) {
                    method.invoke(clazzObj, adaptorGenObj(paramClassArrs[0]), adaptorGenObj(paramClassArrs[1]));
                } else if (paramClassArrs.length == 3) {
                    method.invoke(clazzObj, adaptorGenObj(paramClassArrs[0]), adaptorGenObj(paramClassArrs[1]),
                            adaptorGenObj(paramClassArrs[2]));
                } else if (paramClassArrs.length == 4) {
                    method.invoke(clazzObj, adaptorGenObj(paramClassArrs[0]), adaptorGenObj(paramClassArrs[1]),
                            adaptorGenObj(paramClassArrs[2]), adaptorGenObj(paramClassArrs[3]));
                }
            }
        }
    }

    /**
     * 功能描述: 执行getset方法,先执行set，获取set执行get<br>
     * 〈功能详细描述〉
     *
     * @param classes
     * @param clazzObj
     * @param method
     * @param paramClassArrs
     * @param clazzMethodName
     * @param methodName
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    private void methodInvokeGetSet(Class<?> classes, Object clazzObj, Method method, Class<?>[] paramClassArrs,
                                    String clazzMethodName, String methodName)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Object getObj = null;
        String methodNameSuffix = methodName.substring(3, methodName.length());
        Method getMethod = null;
        try {
            getMethod = classes.getDeclaredMethod("get" + methodNameSuffix, null);
        } catch (NoSuchMethodException e) {
            // 如果对应的get方法找不到,会有is开头的属性名,其get方法就是其属性名称
            if (null == getMethod) {
                Character firstChar = methodNameSuffix.charAt(0);// 取出第一个字符转小写
                String firstLowerStr = firstChar.toString().toLowerCase();
                try {
                    getMethod = classes.getDeclaredMethod(
                            firstLowerStr + methodNameSuffix.substring(1, methodNameSuffix.length()), null);
                } catch (NoSuchMethodException e2) {
                    // 如果还是空的,就跳过吧
                    if (null == getMethod) {
                        return;
                    }
                }
            }
        }
        // 如果get返回结果和set参数结果一样,才可以执行,否则不可以执行
        Class<?> returnClass = getMethod.getReturnType();
        if (paramClassArrs.length == 1 && paramClassArrs[0].toString().equals(returnClass.toString())) {
            getObj = getMethod.invoke(clazzObj, null);
            method.invoke(clazzObj, getObj);
        }

    }

    /**
     * 功能描述: 构造函数构造对象<br>
     * 〈功能详细描述〉
     *
     * @param constructorArr
     * @param classes
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    @SuppressWarnings("rawtypes")
    private Object newConstructor(Constructor[] constructorArr, Class<?> classes)
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        if (null == constructorArr || constructorArr.length < 1) {
            return null;
        }
        Object clazzObj = null;
        boolean isExitNoParamConstruct = false;
        for (Constructor constructor : constructorArr) {
            Class[] constructParamClazzArr = constructor.getParameterTypes();
            //判断是否为无参构造函数
            if (null == constructParamClazzArr || constructParamClazzArr.length == 0) {
                constructor.setAccessible(true);
                clazzObj = classes.newInstance();
                isExitNoParamConstruct = true;
                break;
            }
        }
        // 没有无参构造取第一个
        if (!isExitNoParamConstruct) {
            boolean isContinueFor = false;
            Class[] constructParamClazzArr = constructorArr[0].getParameterTypes();
            Object[] construParamObjArr = new Object[constructParamClazzArr.length];
            for (int i = 0; i < constructParamClazzArr.length; i++) {
                Class constructParamClazz = constructParamClazzArr[i];
                construParamObjArr[i] = adaptorGenObj(constructParamClazz);
                if (null == construParamObjArr[i]) {
                    isContinueFor = true;
                }
            }
            if (!isContinueFor) {
                clazzObj = constructorArr[0].newInstance(construParamObjArr);
            }
        }
        return clazzObj;
    }

    /**
     * 功能描述: 根据类的不同，进行不同实例化<br>
     * 〈功能详细描述〉
     *
     * @param clazz
     * @return
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    private Object adaptorGenObj(Class<?> clazz)
            throws IllegalArgumentException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (null == clazz) {
            return null;
        }
        if ("int".equals(clazz.getName())) {
            return 1;
        } else if ("char".equals(clazz.getName())) {
            return 'x';
        } else if ("boolean".equals(clazz.getName())) {
            return true;
        } else if ("double".equals(clazz.getName())) {
            return 1.0;
        } else if ("float".equals(clazz.getName())) {
            return 1.0f;
        } else if ("long".equals(clazz.getName())) {
            return 1L;
        } else if ("byte".equals(clazz.getName())) {
            return 0xFFFFFFFF;
        } else if ("java.lang.Class".equals(clazz.getName())) {
            return this.getClass();
        } else if ("java.math.BigDecimal".equals(clazz.getName())) {
            return new BigDecimal(1);
        } else if ("java.lang.String".equals(clazz.getName())) {
            return "333";
        } else if ("java.util.Hashtable".equals(clazz.getName())) {
            return new Hashtable<>();
        } else if ("java.util.HashMap".equals(clazz.getName())) {
            return new HashMap<>();
        } else if ("java.util.List".equals(clazz.getName())) {
            return new ArrayList<>();
        } else {
            // 如果是接口或抽象类,直接跳过
            boolean paramIsAbstract = Modifier.isAbstract(clazz.getModifiers());
            boolean paramIsInterface = Modifier.isInterface(clazz.getModifiers());
            if (paramIsInterface || paramIsAbstract) {
                return null;
            }
            Constructor<?>[] constructorArrs = clazz.getConstructors();
            return newConstructor(constructorArrs, clazz);
        }
    }

    /**
     * 功能描述: 获取包下的所有类<br>
     * 〈功能详细描述〉
     *
     * @param packageName
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    private List<Class<?>> getClasses(String packageName) {
        // 第一个class类的集合
        List<Class<?>> classes = new ArrayList<Class<?>>();
        // 是否循环迭代
        boolean recursive = true;
        // 获取包的名字 并进行替换
        String packageDirName = packageName.replace('.', '/');
        // 定义一个枚举的集合 并进行循环来处理这个目录下的things
        Enumeration<URL> dirs;
        try {
            dirs = Thread.currentThread().getContextClassLoader().getResources(packageDirName);
            // 循环迭代下去
            while (dirs.hasMoreElements()) {
                // 获取下一个元素
                URL url = dirs.nextElement();
                // 得到协议的名称
                String protocol = url.getProtocol();
                // 如果是以文件的形式保存在服务器上
                if ("file".equals(protocol)) {
                    // 获取包的物理路径
                    String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
                    // 以文件的方式扫描整个包下的文件 并添加到集合中
                    findAndAddClassesInPackageByFile(packageName, filePath, recursive, classes);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return classes;
    }

    private void findAndAddClassesInPackageByFile(String packageName, String packagePath, final boolean recursive,
                                                  List<Class<?>> classes) {
        // 获取此包的目录 建立一个File
        File dir = new File(packagePath);
        // 如果不存在或者 也不是目录就直接返回
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }
        // 如果存在 就获取包下的所有文件 包括目录
        File[] dirfiles = dir.listFiles(new FileFilter() {
            // 自定义过滤规则 如果可以循环(包含子目录) 或则是以.class结尾的文件(编译好的java类文件)
            public boolean accept(File file) {
                return (recursive && file.isDirectory()) || (file.getName().endsWith(".class"));
            }
        });
        // 循环所有文件
        for (File file : dirfiles) {
            // 如果是目录 则递归继续扫描
            if (file.isDirectory()) {
                findAndAddClassesInPackageByFile(packageName + "." + file.getName(), file.getAbsolutePath(), recursive,
                        classes);
            } else {
                // 如果是java类文件 去掉后面的.class 只留下类名
                String className = file.getName().substring(0, file.getName().length() - 6);
                String pakClazzName = packageName + '.' + className;
                if (filterClazzList.contains(pakClazzName)) {
                    continue;
                }
                try {
                    // 添加到集合中去
                    classes.add(Class.forName(pakClazzName));
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
