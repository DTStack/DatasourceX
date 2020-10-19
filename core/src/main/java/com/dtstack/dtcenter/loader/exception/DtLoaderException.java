package com.dtstack.dtcenter.loader.exception;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:01 2020/1/3
 * @Description：定义 Loader 运行时异常
 */
public class DtLoaderException extends RuntimeException {
    private String errorMessage;

    public DtLoaderException(String errorMessage) {
        super(errorMessage);
        this.errorMessage = errorMessage;
    }

    public DtLoaderException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.errorMessage = errorMessage;
    }
}
