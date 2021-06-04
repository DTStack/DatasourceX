package com.dtstack.dtcenter.loader.client.common;

import com.dtstack.dtcenter.common.loader.common.utils.DateUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Date;

/**
 * 时间工具类测试
 *
 * @author ：wangchuan
 * date：Created in 上午11:37 2021/6/4
 * company: www.dtstack.com
 */
public class DateUtilTest {

    /**
     * 获取当前时间戳
     */
    @Test
    public void getCurrentSeconds() {
        DateUtil.getCurrentSeconds();
    }

    /**
     * 获取SimpleDateFormat
     */
    @Test
    public void getDateFormat() {
        Assert.assertNotNull(DateUtil.getDateFormat("H"));
    }

    /**
     * 根据时间戳，格式化为日期时间字符串 yyyy-MM-dd HH:mm:ss
     */
    @Test
    public void getFormattedDate() {
        Assert.assertNotNull(DateUtil.getFormattedDate(System.currentTimeMillis()));
    }

    /**
     * 根据日期，获取时间戳 yyyy-MM-dd HH:mm:ss
     */
    @Test
    public void getTimestamp() {
        DateUtil.getTimestamp("2021-06-04");
    }

    /**
     * 根据时间戳，获得当天的开始时间戳
     */
    @Test
    public void getTimestamp2() {
        DateUtil.getTimestamp(System.currentTimeMillis());
    }

    /**
     * 前 1 天的时间戳
     */
    @Test
    public void todayLess() {
        DateUtil.todayLess(1);
    }

    /**
     * 明天0点的时间戳
     */
    @Test
    public void tomorrowZero() {
        DateUtil.tomorrowZero();
    }

    /**
     * 明天0点的时间戳
     */
    @Test
    public void getTimeStrWithoutSymbol() {
        Assert.assertEquals("1", DateUtil.getTimeStrWithoutSymbol("-: 1"));
    }

    /**
     * 将yyyyMMddHHmmss ---> yyyy-MM-dd HH:mm:ss
     */
    @Test
    public void addTimeSplit() {
        Assert.assertEquals("yyyy-MM-dd HH:mm:ss", DateUtil.addTimeSplit("yyyyMMddHHmmss"));
        Assert.assertEquals("yyyyMMddHHmmss2", DateUtil.addTimeSplit("yyyyMMddHHmmss2"));
    }

    /**
     * 获取时间差
     */
    @Test
    public void getTimeDifference() {
        long currentTimeMillis = System.currentTimeMillis();
        Timestamp currentTimestamp = new Timestamp(currentTimeMillis);
        Timestamp dif1 = new Timestamp(currentTimeMillis + 500);
        Timestamp dif2 = new Timestamp(currentTimeMillis + 5 * 1000 * 60 * 60 + 5 * 1000 * 60 + 5 * 1000);
        Assert.assertEquals("0秒", DateUtil.getTimeDifference(currentTimestamp, currentTimestamp));
        Assert.assertEquals("500毫秒", DateUtil.getTimeDifference(currentTimestamp, dif1));
        Assert.assertEquals("5小时5分钟5秒", DateUtil.getTimeDifference(currentTimestamp, dif2));
    }

    /**
     * 获取当前年份
     */
    @Test
    public void getNowYear() {
        int nowYear = DateUtil.getNowYear();
        Assert.assertTrue(nowYear > 2020);
    }

    /**
     * 获取当前月份
     */
    @Test
    public void getNowMonth() {
        int nowYear = DateUtil.getNowMonth();
        Assert.assertTrue(nowYear > 0);
    }

    /**
     * 获取几天前的时间戳
     */
    @Test
    public void getLastDay() {
        long lastDay = DateUtil.getLastDay(1);
        Assert.assertTrue(lastDay > 0);
    }

    /**
     * 获取毫秒
     */
    @Test
    public void getMillisecond() {
        Assert.assertEquals(Long.valueOf(1001L), DateUtil.getMillisecond(1001L));
    }

    /**
     * 获取 timestamp
     */
    @Test
    public void getSqlTimeStampVal() {
        Assert.assertNull(DateUtil.getSqlTimeStampVal(null));
        Assert.assertNotNull(DateUtil.getSqlTimeStampVal("2021-02-01 17:58:01"));
        Assert.assertNotNull(DateUtil.getSqlTimeStampVal("2021-02-01"));
        Assert.assertNotNull(DateUtil.getSqlTimeStampVal("17:58:01"));
        Assert.assertNotNull(DateUtil.getSqlTimeStampVal("2021"));
        Assert.assertNotNull(DateUtil.getSqlTimeStampVal(1000));
        Assert.assertNotNull(DateUtil.getSqlTimeStampVal(new Date()));
        Assert.assertNotNull(DateUtil.getSqlTimeStampVal(new Timestamp(System.currentTimeMillis())));
    }

    /**
     * 获取 timestamp 异常
     */
    @Test(expected = Exception.class)
    public void getSqlTimeStampValErr() {
        Assert.assertNull(DateUtil.getSqlTimeStampVal(this.getClass()));
    }
}
