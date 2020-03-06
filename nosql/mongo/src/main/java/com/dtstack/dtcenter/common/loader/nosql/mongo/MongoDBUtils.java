package com.dtstack.dtcenter.common.loader.nosql.mongo;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.util.AddressUtil;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:28 2020/2/5
 * @Description：MongoDB 工具类
 */
@Slf4j
public class MongoDBUtils {
    private static final String HOST_SPLIT_REGEX = ",\\s*";

    private static Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>(.*)):((?<port>\\d+))*");

    private static final Integer DEFAULT_PORT = 27017;

    public static final int TIME_OUT = 5 * 1000;

    public static boolean checkConnection(SourceDTO source) {
        boolean check = false;
        MongoClient mongoClient = null;
        try {
            MongoClientOptions options = MongoClientOptions.builder()
                    .serverSelectionTimeout(TIME_OUT)
                    .build();

            List<ServerAddress> serverAddress = getServerAddress(source.getUrl());

            if (StringUtils.isEmpty(source.getUsername()) || StringUtils.isEmpty(source.getPassword())) {
                mongoClient = new MongoClient(serverAddress, options);
            } else {
                MongoCredential credential = MongoCredential.createScramSha1Credential(source.getUsername(),
                        source.getSchema(), source.getPassword().toCharArray());
                List<MongoCredential> credentials = Lists.newArrayList();
                credentials.add(credential);

                mongoClient = new MongoClient(serverAddress, credentials, options);
            }
            MongoDatabase mongoDatabase = mongoClient.getDatabase(source.getSchema());
            MongoIterable<String> mongoIterable = mongoDatabase.listCollectionNames();
            mongoIterable.iterator().next();
            check = true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
        return check;
    }

    protected static List<ServerAddress> getServerAddress(String hostPorts) {
        List<ServerAddress> addresses = Lists.newArrayList();

        boolean isTelnet = true;
        StringBuilder errorHost = new StringBuilder();
        for (String hostPort : hostPorts.split(HOST_SPLIT_REGEX)) {
            if (hostPort.length() == 0) {
                continue;
            }

            Matcher matcher = HOST_PORT_PATTERN.matcher(hostPort);
            if (matcher.find()) {
                String host = matcher.group("host");
                String portStr = matcher.group("port");
                int port = portStr == null ? DEFAULT_PORT : Integer.parseInt(portStr);

                if (!AddressUtil.telnet(host, port)) {
                    errorHost.append(hostPort).append(" ");
                    isTelnet = false;
                }

                ServerAddress serverAddress = new ServerAddress(host, port);
                addresses.add(serverAddress);
            }
        }

        if (!isTelnet) {
            throw new DtCenterDefException("连接信息：" + errorHost.toString(), DBErrorCode.IP_PORT_CONN_ERROR);
        }

        return addresses;
    }
}
