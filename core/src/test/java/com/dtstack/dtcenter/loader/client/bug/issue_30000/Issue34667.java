package com.dtstack.dtcenter.loader.client.bug.issue_30000;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * bug描述：keyberos配置文件zip包中如果有目录会报错
 *
 * bug连接：http://redmine.prod.dtstack.cn/issues/34667
 *
 * @author ：wangchuan
 * date：Created in 2:58 下午 2021/1/13
 * company: www.dtstack.com
 */
public class Issue34667 {

    private static final HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://krbt3:10000/default;principal=hdfs/krbt3@DTSTACK.COM")
            .defaultFS("hdfs://krbt1:8020")
            .build();

    @Test
    public void test_for_issue () throws Exception{
        String localKerberosPath = Issue34667.class.getResource("/bug/issue_34667/").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.HIVE.getVal());
        Map<String, Object> kerberosMap = kerberos.parseKerberosFromUpload(localKerberosPath + "kerberos_dir.zip", localKerberosPath);
        kerberos.prepareKerberosForConnect(kerberosMap, localKerberosPath);
        source.setKerberosConfig(kerberosMap);
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Assert.assertTrue(client.testCon(source));
    }
}
