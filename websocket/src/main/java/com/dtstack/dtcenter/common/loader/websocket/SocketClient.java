package com.dtstack.dtcenter.common.loader.websocket;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.WebSocketSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.java_websocket.WebSocket;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:01 2020/11/25
 * @Description：Socket 客户端
 */
@Slf4j
public class SocketClient implements IClient {
    /**
     * socket 地址
     */
    private static final String SOCKET_URL = "%s?%s";

    @Override
    public Boolean testCon(ISourceDTO source) {
        WebSocketSourceDTO socketSourceDTO = (WebSocketSourceDTO) source;
        String authParamStr = null;
        if (MapUtils.isNotEmpty(socketSourceDTO.getAuthParams())) {
            authParamStr = socketSourceDTO.getAuthParams().entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.joining("&"));
        }
        WebSocketClient myClient = null;
        try {
            String url = StringUtils.isNotBlank(authParamStr) ? String.format(SOCKET_URL, socketSourceDTO.getUrl(), authParamStr) : socketSourceDTO.getUrl();
            myClient = new WebSocketClient(new URI(url)) {
                @Override
                public void onOpen(ServerHandshake handshakedata) {
                    log.info("SOCKET 连接成功");
                }

                @Override
                public void onMessage(String message) {
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    log.info("SOCKET 关闭成功");
                }

                @Override
                public void onError(Exception ex) {
                    throw new DtLoaderException(ex.getMessage(), ex);
                }
            };
            myClient.connect();
            // 判断是否连接成功，未成功后面发送消息时会报错
            int maxRetry = 0;
            while (!WebSocket.READYSTATE.OPEN.equals(myClient.getReadyState()) && maxRetry < 5) {
                maxRetry++;
                Thread.sleep(1000);
            }
            return WebSocket.READYSTATE.OPEN.equals(myClient.getReadyState());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (myClient != null) {
                myClient.close();
            }
        }
        return false;
    }

    @Override
    public Connection getCon(ISourceDTO source) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public String getTableMetaComment(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public Table getTable(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public String getCurrentDatabase(ISourceDTO source) {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public Boolean createDatabase(ISourceDTO source, String dbName, String comment) throws Exception {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public Boolean isDatabaseExists(ISourceDTO source, String dbName) throws Exception {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) throws Exception {
        throw new DtLoaderException("该数据源暂时不支持该方法!");
    }
}
