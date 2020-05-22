package com.dtstack.dtcenter.common.loader.greenplum;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Greenplum6SourceDTO;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 19:45 2020/4/13
 * @Description：Greenplum 测试
 */
public class GreenplumClientTest {
    private static AbsRdbmsClient rdbsClient = new GreenplumClient();

    private static final String TABLE_QUERY = "select c.relname as tablename" +
            " from pg_catalog.pg_class c, pg_catalog.pg_namespace n" +
            " where" +
            " n.oid = c.relnamespace" +
            " and n.nspname='%s'";

    @Test
    public void getConnFactory() throws Exception {
        Greenplum6SourceDTO source = Greenplum6SourceDTO.builder()
                .url("jdbc:pivotal:greenplum://172.16.10.90:5432;DatabaseName=data")
                .username("gpadmin")
                .password("gpadmin")
                .schema("public")
                .build();
        List<String> tableList = rdbsClient.getTableList(source, null);
//        Connection connection = rdbsClient.getCon(source);
//        ResultSet rs=connection.createStatement().executeQuery(String.format(TABLE_QUERY,"pg_catalog"));
//        while (rs.next()) {
//            System.out.println(rs.getString(1));
//        }
        System.out.println(tableList.size());
        assert tableList != null;
    }

    @Test
    public void testSchema() throws Exception {
        Greenplum6SourceDTO source = Greenplum6SourceDTO.builder()
                .url("jdbc:pivotal:greenplum://172.16.10.90:5432;DatabaseName=data")
                .username("gpadmin")
                .password("gpadmin")
                .schema("test")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("CREATE TABLE if not exists nanqi100 ( id integer )").build();
        rdbsClient.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void testTableMetadata() throws Exception {
        Greenplum6SourceDTO source = Greenplum6SourceDTO.builder()
                .url("jdbc:pivotal:greenplum://172.16.10.90:5432;DatabaseName=data")
                .username("gpadmin")
                .password("gpadmin")
                .schema("test")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("student").build();
        String metaComment = rdbsClient.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }


}
