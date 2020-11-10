package com.dtstack.dtcenter.common.loader.common.service;

import com.dtstack.dtcenter.common.loader.common.exception.ConnErrorCode;
import com.dtstack.dtcenter.common.loader.common.exception.IErrorPattern;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 数据库错误分析实现类
 *
 * @author ：wangchuan
 * date：Created in 上午10:54 2020/11/6
 * company: www.dtstack.com
 */
public class ErrorAdapterImpl implements IErrorAdapter {

    @Override
    public String connAdapter(String errorMsg, IErrorPattern errorPattern){
        for (ConnErrorCode errorCode : ConnErrorCode.values()){
            Pattern connErrorPattern = errorPattern.getConnErrorPattern(errorCode.getCode());
            if (Objects.isNull(connErrorPattern)) {
                continue;
            }
            Matcher matcher = connErrorPattern.matcher(errorMsg);
            if (matcher.find()) {
                return errorCode.getDesc();
            }
        }
        // 未定义该异常
        return ConnErrorCode.UNDEFINED_ERROR.getDesc();
    }

    @Override
    public String sqlAdapter(String errorMsg, IErrorPattern errorPattern) {
        // TODO 预留，后期优化
        return "";
    }
}
