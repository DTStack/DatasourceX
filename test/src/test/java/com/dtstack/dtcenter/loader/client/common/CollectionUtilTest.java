package com.dtstack.dtcenter.loader.client.common;

import com.dtstack.dtcenter.common.loader.common.utils.CollectionUtil;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/**
 * 集合工具类测试
 *
 * @author ：wangchuan
 * date：Created in 上午11:31 2021/6/4
 * company: www.dtstack.com
 */
public class CollectionUtilTest {

    /**
     * 集合转 string
     */
    @Test
    public void listToStr() {
        String toStr = CollectionUtil.listToStr(Lists.newArrayList("a", "b", "", "d"));
        Assert.assertEquals(toStr, "a,b,d");
    }
}
