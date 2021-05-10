import com.dtstack.dtcenter.common.loader.impala.ImpalaClient;
import com.dtstack.dtcenter.common.loader.impala.ImpalaConnFactory;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;


public class ImpalaTest {


    @Test
    public void getCon() throws Exception {
        ImpalaSourceDTO source = ImpalaSourceDTO.builder()
                .url("jdbc:impala://172.16.101.17:21050/default")
                .defaultFS("hdfs://ns1")
                .build();

        ImpalaConnFactory connFactory = new ImpalaConnFactory();
        connFactory.getConn(source, StringUtils.EMPTY);
    }

    @Test
    public void getTableList() throws Exception {
        ImpalaSourceDTO source = ImpalaSourceDTO.builder()
                .url("jdbc:impala://172.16.101.17:21050/default")
                .defaultFS("hdfs://ns1")
                .schema("aa_aa")
                .build();
        IClient client = new ImpalaClient();
        List<String> list = client.getTableList(source, SqlQueryDTO.builder().build());
        System.out.println(list);
    }
}
