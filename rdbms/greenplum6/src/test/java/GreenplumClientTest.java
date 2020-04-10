import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.greenplum.GreenplumClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:21 2020/4/10
 * @Description：Greenplum 测试
 */
public class GreenplumClientTest {
    private static AbsRdbmsClient rdbsClient = new GreenplumClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:pivotal:greenplum://172.16.10.90:5432;DatabaseName=template1")
                .username("gpadmin")
                .password("gpadmin")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}
