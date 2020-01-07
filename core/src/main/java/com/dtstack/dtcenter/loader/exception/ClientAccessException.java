package com.dtstack.dtcenter.loader.exception;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:55 2020/1/3
 * @Description：插件化异常
 */
public class ClientAccessException extends DtLoaderException {
    private static final String CLIENT_INIT_EXCEPTION = "Client access exception. ";

    public ClientAccessException(Throwable cause) {
        super(CLIENT_INIT_EXCEPTION, cause);
    }
}
