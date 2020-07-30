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
     * 是否开启缓存 ：对odps和非关系型数据库有效
     * @return
     */
    Boolean getIsCache();

}
