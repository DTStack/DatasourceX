package com.dtstack.dtcenter.loader.client.clickhouse;

import com.dtstack.dtcenter.common.loader.clickhouse.ClickhouseAdapter;
import com.dtstack.dtcenter.common.loader.kingbase.KingbaseAdapter;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class AdapterTest {

    @Test
    public void test() throws ClassNotFoundException, IllegalAccessException {
        //获取所有变量的值
        Class clazz = Class.forName("java.sql.Types");
        Field[] fields = clazz.getFields();
        Assert.assertEquals(39, fields.length);
        for( Field field : fields ){
            ClickhouseAdapter.mapColumnTypeJdbc2Java(field.getInt(clazz),1,1);
            KingbaseAdapter.mapColumnTypeJdbc2Java(field.getInt(clazz),1,1);
        }
        ClickhouseAdapter.mapColumnTypeJdbc2Oracle(1,1,1);
        KingbaseAdapter.mapColumnTypeJdbc2Oracle(1,1,1);
    }

}
