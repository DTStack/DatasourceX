package com.dtstack.dtcenter.common.loader.odps;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
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
        Boolean testCon = client.testCon(sourceDTO);
        assert testCon != null;
    }

    @Test
    public void getTableList() throws Exception {
        List tableList = client.getTableList(sourceDTO, null);
        assert tableList != null;
    }

    @Test
    public void getColumnMetaData() throws Exception {
        List tableList = client.getColumnMetaData(sourceDTO, SqlQueryDTO.builder().tableName("aa_temp").build());
        assert tableList != null;
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        List tableList = client.getColumnClassInfo(sourceDTO, SqlQueryDTO.builder().tableName("aa_temp").build());
        assert tableList != null;
    }

    @Test
    public void executeQuery () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> mapList = client.executeQuery(sourceDTO, queryDTO);
        assert mapList != null;
    }

    @Test
    public void executeSqlWithoutResultSet () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("insert into nanqi (id) values(1), (2), (3)").build();
        Boolean result = client.executeSqlWithoutResultSet(sourceDTO, queryDTO);
        assert result != null;
    }

    @Test
    public void getPreview() {
        List tableList = client.getPreview(sourceDTO, SqlQueryDTO.builder().tableName("act_hi_actinst").build());
        assert tableList != null;
    }

    @Test
    public void getTableMetaComment() throws Exception {
        String aa_temp = client.getTableMetaComment(sourceDTO, SqlQueryDTO.builder().tableName("aa_temp").build());
        assert aa_temp != null;
    }
}