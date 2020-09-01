package com.dtstack.dtcenter.loader.dto.source;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:23 2020/5/22
 * @Description：数据源接口
 */
public interface ISourceDTO {
    /**
     * 获取用户名
     *
     * @return
     */
    String getUsername();

    /**
     * 获取密码
     *
     * @return
     */
    String getPassword();

    /**
     * 获取数据源类型
     *
     * @return
     */
    Integer getSourceType();

}
