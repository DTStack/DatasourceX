package com.dtstack.dtcenter.common.loader.tdbs.hdfs;

import com.dtstack.dtcenter.common.loader.common.exception.AbsErrorPattern;
import com.dtstack.dtcenter.common.loader.common.exception.ConnErrorCode;

import java.util.regex.Pattern;

/**
 *
 * @author ：wangchuan
 * date：Created in 下午1:46 2020/11/6
 * company: www.dtstack.com
 */
public class HDFSErrorPattern extends AbsErrorPattern {

    private static final Pattern DB_PERMISSION_ERROR = Pattern.compile("(?i)Permission\\s*denied");

    static {
        PATTERN_MAP.put(ConnErrorCode.DB_PERMISSION_ERROR.getCode(), DB_PERMISSION_ERROR);
    }
}
