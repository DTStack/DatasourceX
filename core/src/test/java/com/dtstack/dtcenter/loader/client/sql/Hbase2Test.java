package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.comparator.BinaryComparator;
import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:43 2020/7/9
 * @Description：Hbase 2 测试
 */
public class Hbase2Test {
    PoolConfig poolConfig = PoolConfig.builder().maximumPoolSize(100).build();

    HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("kudu1,kudu2,kudu3:2181")
            .path("/hbase")
            .poolConfig(poolConfig)
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
        List<String> tableList = client.getTableList(source, null);
        System.out.println(tableList);
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
        List dtstack = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("TEST").build());
        System.out.println(dtstack);
    }

    @Test
    public void executorQuery() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(100, new RdosThreadFactory("stream_metrics_query"));
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
//        PageFilter pageFilter = new PageFilter(2);
        ArrayList<Filter> filters = new ArrayList<>();
//        filters.add(pageFilter);
//        String column = "baseInfo:age";
//        String column2 = "liftInfo:girlFriend";
//        ArrayList<Object> columns = Lists.newArrayList(column, column2);
//        SingleColumnValueFilter filter = new SingleColumnValueFilter("baseInfo".getBytes(), "age".getBytes(), CompareOp.EQUAL, new RegexStringComparator("."));
//        filter.setFilterIfMissing(true);
//        filters.add(filter);
        RowFilter hbaseRowFilter = new RowFilter(
                CompareOp.EQUAL,new BinaryComparator("0_k".getBytes()));
        hbaseRowFilter.setReversed(true);
        System.out.println(System.currentTimeMillis());
        List list1 = client.executeQuery(source, SqlQueryDTO.builder().tableName("wuren_foo").hbaseFilter(filters).build());
        System.out.println(System.currentTimeMillis());
        List list2 = client.executeQuery(source, SqlQueryDTO.builder().tableName("wuren_foo").hbaseFilter(filters).build());
        System.out.println(System.currentTimeMillis());
        List list3 = client.executeQuery(source, SqlQueryDTO.builder().tableName("wuren_foo").hbaseFilter(filters).build());
        System.out.println(System.currentTimeMillis());
        CountDownLatch latch = new CountDownLatch(1200);
//        filters.add(pageFilter);
        filters.add(hbaseRowFilter);
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("wuren_foo").hbaseFilter(filters).build();
        List<FirstThread> list = new ArrayList<>();
        for(int i = 0;i<1200;i++){
            FirstThread firstThread = new FirstThread(client,sqlQueryDTO,source,latch);
            list.add(firstThread);
        }
        List<Future<?>> futureList = Lists.newArrayList();
        for (FirstThread firstThread : list) {
            Future<Object> future = executorService.submit(firstThread);
            futureList.add(future);
        }
        latch.await();
        System.out.println(list);
    }

    public class RdosThreadFactory implements ThreadFactory {
        private  final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
        private  final AtomicInteger THREAD_NUMBER = new AtomicInteger(1);
        private final ThreadGroup group;
        private final String namePrefix;

        public RdosThreadFactory(String factoryName) {
            SecurityManager s = System.getSecurityManager();
            this.group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = factoryName + "-pool-" + POOL_NUMBER.getAndIncrement() + "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(this.group, r, this.namePrefix + THREAD_NUMBER.getAndIncrement(), 0L);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }

            if (t.getPriority() != 5) {
                t.setPriority(5);
            }

            return t;
        }
    }

    public class FirstThread implements Callable {
        private IClient client;
        private SqlQueryDTO queryDTO;
        private HbaseSourceDTO hbaseSourceDTO;
        private CountDownLatch latch;
        public FirstThread(IClient client, SqlQueryDTO queryDTO,HbaseSourceDTO hbaseSourceDTO,CountDownLatch latch) {
            this.client = client;
            this.queryDTO = queryDTO;
            this.hbaseSourceDTO = hbaseSourceDTO;
            this.latch = latch;
        }

        @Override
        public Object call() throws Exception {
            try {
                Long current = System.currentTimeMillis();
                client.executeQuery(source,queryDTO);
                Long current1 = System.currentTimeMillis();
                System.out.println(current1-current);
                return null;
            } finally {
                if (latch != null)
                    latch.countDown();
            }
        }
    }

    @Test
    public void preview() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("TEST").previewNum(1).build());
        System.out.println(result);
    }
}
