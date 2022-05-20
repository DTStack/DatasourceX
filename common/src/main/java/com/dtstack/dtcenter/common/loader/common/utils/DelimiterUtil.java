package com.dtstack.dtcenter.common.loader.common.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * 分隔符工具类
 *
 * @author ：wangchuan
 * date：Created in 下午4:35 2022/5/20
 * company: www.dtstack.com
 */
public class DelimiterUtil {

    /**
     * 取首字符分隔符, 忽略转义
     *
     * @param delimiter 分隔符
     * @return 分隔符
     */
    public static String charAtIgnoreEscape(String delimiter) {
        if (StringUtils.isEmpty(delimiter)) {
            return null;
        }
        if (delimiter.length() > 1 && delimiter.charAt(0) == '\\') {
            return delimiter.substring(0, 2);
        }
        return delimiter.substring(0, 1);
    }
}
