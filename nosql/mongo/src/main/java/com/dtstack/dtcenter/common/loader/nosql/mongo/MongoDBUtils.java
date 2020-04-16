package com.dtstack.dtcenter.common.loader.nosql.mongo;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.util.AddressUtil;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.net.URLEncoder;
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

    private static Pattern USER_PWD_PATTERN = Pattern.compile("(?<username>(.*)):(?<password>(.*))@(?<else>(.*))");

    private static final Integer DEFAULT_PORT = 27017;

    public static final int TIME_OUT = 5 * 1000;

    public static boolean checkConnection(SourceDTO source) {
        boolean check = false;
        MongoClient mongoClient = null;
        try {
            mongoClient = getClient(source.getHostPort(), source.getUsername(), source.getPassword(),source.getSchema());
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

    public static List<String> getTableList(SourceDTO source) {
        List<String> tableList = Lists.newArrayList();
        MongoClient mongoClient = null;
        try {
            String db = source.getSchema();
            mongoClient = getClient(source.getHostPort(), source.getUsername(), source.getPassword(), db);
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoIterable<String> tableNames = mongoDatabase.listCollectionNames();
            for (String s : tableNames) {
                tableList.add(s);
            }
        } catch (Exception e) {
            log.error("获取tablelist异常  {}", source, e);
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }

        return tableList;
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


    /*
    1.  username:password@host:port,host:port/db?option
    2.  host:port,host:port/db?option
     */

    public static MongoClient getClient(String hostPorts, String username, String password, String db) {
        MongoClient mongoClient = null;
        hostPorts = hostPorts.trim();
        MongoClientOptions options = MongoClientOptions.builder()
                .serverSelectionTimeout(TIME_OUT)
                .build();
        Matcher matcher = USER_PWD_PATTERN.matcher(hostPorts);
        if (matcher.matches()) {
            String usernameUrl = matcher.group("username");
            String passwordUrl = matcher.group("password");
            String elseUrl = matcher.group("else");
            MongoClientURI clientURI;
            StringBuilder uri = new StringBuilder();
            uri.append(String.format("mongodb://%s:%s@%s", URLEncoder.encode(usernameUrl), URLEncoder.encode(passwordUrl), elseUrl));
            clientURI = new MongoClientURI(uri.toString());
            mongoClient = new MongoClient(clientURI);
        } else {
            List<ServerAddress> serverAddress = getServerAddress(hostPorts);
            if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
                mongoClient = new MongoClient(serverAddress, options);
            } else {
                MongoCredential credential = MongoCredential.createScramSha1Credential(username, db, password.toCharArray());
                List<MongoCredential> credentials = Lists.newArrayList();
                credentials.add(credential);
                mongoClient = new MongoClient(serverAddress, credentials, options);
            }
        }
        return mongoClient;
    }
}
