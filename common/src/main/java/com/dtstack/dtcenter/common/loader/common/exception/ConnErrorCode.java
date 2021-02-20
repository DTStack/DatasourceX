package com.dtstack.dtcenter.common.loader.common.exception;

/**
 * 获取连接时出现的错误码和相关描述集合
 *
 * @author ：wangchuan
 * date：Created in 上午10:16 2020/11/6
 * company: www.dtstack.com
 */
public enum ConnErrorCode implements IErrorCode {

    USERNAME_PASSWORD_ERROR(1, "数据库账号或密码错误"),

    MISSING_USERNAME_OR_PASSWORD(2, "缺少账号名或密码"),

    DB_NOT_EXISTS(3, "数据库不存在"),

    UNKNOWN_HOST_ERROR(4, "不能识别该host域名，请检查host配置"),

    JDBC_FORMAT_ERROR(5, "jdbcUrl格式错误"),

    CONNECTION_TIMEOUT(6, "数据库连接超时"),

    IP_PORT_FORMAT_ERROR(7, "ip或端口错误"),

    VERSION_ERROR(8, "数据库版本错误，请检查使用版本是否对应"),

    DB_PERMISSION_ERROR(9, "此用户无访问权限"),

    CANNOT_ACQUIRE_CONNECT(10, "应用程序服务器拒绝建立连接，请检查网络是否通畅"),

    CANNOT_PING_IP(11, "此ip拒绝连接"),

    CANNOT_TELNET_PORT(12, "此端口拒绝连接"),

    HDFS_PERMISSION_ERROR(13, "hdfs权限检查失败，请检查该用户权限"),

    ZK_NODE_NOT_EXISTS(14, "zookeeper 上不存在此节点信息，请检查znode是否正确"),

    ZK_IS_NOT_CONNECT(15, "zookeeper 服务拒绝连接，请检查zk地址、端口是否正确或zookeeper服务是否正常"),

    /**
     * 未定义异常
     */
    UNDEFINED_ERROR(0, "获取数据库连接失败");

    @Override
    public Integer getCode() {
        return code;
    }

    @Override
    public String getDesc() {
        return desc;
    }

    ConnErrorCode(Integer code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    /**
     * 错误码
     */
    private final Integer code;

    /**
     * 错误描述
     */
    private final String desc;
}
