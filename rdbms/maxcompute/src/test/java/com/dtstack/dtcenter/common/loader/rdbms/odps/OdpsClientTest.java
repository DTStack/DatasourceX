package com.dtstack.dtcenter.common.loader.rdbms.odps;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2020/4/8
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
public class OdpsClientTest {
    private AbsRdbmsClient client = new OdpsClient();
    private SourceDTO sourceDTO;
    {
        Map<String,String> odpsConfig = new HashMap<>();
        odpsConfig.put("accessId", "LTAIljBeC8ei9Yy0");
        odpsConfig.put("accessKey", "gwTWasH7sEE0pSUEuiXnw7JecXyfGF");
        odpsConfig.put("project", "dtstack_dev");
        sourceDTO = SourceDTO.builder().config(JSON.toJSONString(odpsConfig)).build();
    }



    @Test
    public void testCon() {
        Boolean aBoolean = client.testCon(sourceDTO);
        System.out.println(aBoolean);
    }

    @Test
    public void getTableList() throws Exception {
        List tableList = client.getTableList(sourceDTO, null);
        System.out.println(tableList);


    }

    @Test
    public void getColumnMetaData() throws Exception {

        List tableList = client.getColumnMetaData(sourceDTO, SqlQueryDTO.builder().tableName("aa_temp").build());

        System.out.println(tableList);
    }

    @Test
    public void getPreview() {

        List tableList = client.getPreview(sourceDTO, SqlQueryDTO.builder().tableName("act_hi_actinst").build());

        System.out.println(tableList);
    }

    @Test
    public void getTableMetaComment() throws Exception {
        String aa_temp = client.getTableMetaComment(sourceDTO, SqlQueryDTO.builder().tableName("aa_temp").build());
        System.out.println(aa_temp);
    }
}