package com.dtstack.dtcenter.loader.utils;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;

/**
 * 断言工具类
 *
 * @author ：wangchuan
 * date：Created in 上午10:25 2021/6/23
 * company: www.dtstack.com
 */
public class AssertUtils {

    public static void isTrue(boolean expression, String message) {
        if (!expression) {
            throw new DtLoaderException(message);
        }
    }

    public static void isOverLength(String content, Integer limit, String message) {
        if (StringUtils.isNotBlank(content) && content.length() > limit) {
            throw new DtLoaderException(message);
        }
    }

    public static void notNull(Object obj, String message) {
        if (obj == null) {
            throw new DtLoaderException(message);
        }
    }

    public static void isNull(Object obj, String message) {
        if (obj != null) {
            throw new DtLoaderException(message);
        }
    }

    public static void notBlank(String obj, String message) {
        if (StringUtils.isBlank(obj)) {
            throw new DtLoaderException(message);
        }
    }

    public static void isFalse(boolean expression, String message) {
        if (expression) {
            throw new DtLoaderException(message);
        }
    }

    public static void notNull(Collection collection, String message) {
        if (CollectionUtils.isEmpty(collection)) {
            throw new DtLoaderException(message);
        }
    }

    public static void isTrue(Response response, Integer code) {
        if (response == null) {
            throw new DtLoaderException("response is null");
        }
        JSONObject jsonObject = JSONObject.parseObject(response.getContent());
        Integer statusCode = jsonObject.getInteger("code");
        AssertUtils.isTrue(200 == response.getStatusCode() && code.equals(statusCode), response.toString());
    }

}
