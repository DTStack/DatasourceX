package com.dtstack.dtcenter.common.loader.odps.pool;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Tables;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.odps.OdpsClient;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午3:15 2020/8/3
 * @Description：
 */
public class OdpsPoolFactory implements PooledObjectFactory<Odps> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OdpsPoolFactory.class);

    private String odpsServer;

    private String accessId;

    private String accessKey;

    private String project;

    private String packageAuthorizedProject;

    private String accountType;

    public OdpsPoolFactory(OdpsPoolConfig config) {
        this.odpsServer = config.getOdpsServer();
        this.accessId = config.getAccessId();
        this.accessKey = config.getAccessKey();
        this.project = config.getProject();
        this.packageAuthorizedProject = config.getPackageAuthorizedProject();
        this.accountType = config.getAccountType();
    }

    /**
     * 当对象池中没有多余的对象可以用的时候，调用此方法。
     * @return
     * @throws Exception
     */
    @Override
    public PooledObject<Odps> makeObject() throws Exception {
        Odps odps = OdpsClient.initOdps(odpsServer, accessId, accessKey, project, packageAuthorizedProject, accountType);
        // 连接池中的连接对象
        return new DefaultPooledObject<>(odps);
    }

    /**
     * 销毁
     * @param pooledObject
     * @throws Exception
     */
    @Override
    public void destroyObject(PooledObject<Odps> pooledObject) throws Exception {
        Odps odps = pooledObject.getObject();
        boolean check = false;
        try {
            Tables tables = odps.tables();
            tables.iterator().hasNext();
            check = true;
        } catch (Exception e) {
            LOGGER.error("检查odps连接失败..{}", e);
        }
        if (Objects.nonNull(odps) && !check) {
            try {
                odps = null;
            } catch (Exception e) {
                LOGGER.error("close client error", e);
                throw new DtCenterDefException("close client error", e);
            }
        }
    }

    /**
     * 功能描述：判断连接对象是否有效，有效返回 true，无效返回 false
     * 什么时候会调用此方法
     * 1：从连接池中获取连接的时候，参数 testOnBorrow 或者 testOnCreate 中有一个 配置 为 true 时，
     * 则调用 factory.validateObject() 方法.
     * 2：将连接返还给连接池的时候，参数 testOnReturn，配置为 true 时，调用此方法.
     * 3：连接回收线程，回收连接的时候，参数 testWhileIdle，配置为 true 时，调用此方法.
     * @param pooledObject
     * @return
     */
    @Override
    public boolean validateObject(PooledObject<Odps> pooledObject) {
        Odps odps = pooledObject.getObject();
        try {
            Tables tables = odps.tables();
            tables.iterator().hasNext();
            return true;
        } catch (Exception e) {
            LOGGER.error("检查odps连接失败..{}", e);
        }
        return false;
    }

    /**
     * 功能描述：激活资源对象
     * 什么时候会调用此方法
     * 1：从连接池中获取连接的时候
     *  2：连接回收线程，连接资源的时候，根据配置的 testWhileIdle 参数，
     *  判断 是否执行 factory.activateObject()方法，true 执行，false 不执行
     * @param pooledObject
     * @throws Exception
     */
    @Override
    public void activateObject(PooledObject<Odps> pooledObject) throws Exception {
        Odps odps = pooledObject.getObject();
        //测试连接一下，使其没有空闲
        try {
            Tables tables = odps.tables();
            tables.iterator().hasNext();
        } catch (Exception e) {
            //do nothing
        }
    }

    /**
     * 功能描述：钝化资源对象
     * 将连接返还给连接池时，调用此方法。
     * @param pooledObject
     * @throws Exception
     */
    @Override
    public void passivateObject(PooledObject<Odps> pooledObject) throws Exception {
        // nothing
    }
}
