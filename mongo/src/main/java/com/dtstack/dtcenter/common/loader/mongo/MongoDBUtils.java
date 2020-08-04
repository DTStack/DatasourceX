package com.dtstack.dtcenter.common.loader.mongo;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.mongo.pool.MongoManager;
import com.dtstack.dtcenter.common.util.AddressUtil;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.MongoSourceDTO;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.util.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    private static Pattern HOST_PORT_SCHEMA_PATTERN = Pattern.compile("(?<host>(.*)):((?<port>\\d+))*/(?<schema>(.*))");

    private static Pattern USER_PWD_PATTERN = Pattern.compile("(?<username>(.*)):(?<password>(.*))@(?<else>(.*))");

    private static final Integer DEFAULT_PORT = 27017;

    public static final int TIME_OUT = 5 * 1000;

    private static final String poolConfigFieldName = "poolConfig";

    private static MongoManager mongoManager = MongoManager.getInstance();

    public static final ThreadLocal<Boolean> isOpenPool = new ThreadLocal<>();

    public static boolean checkConnection(ISourceDTO iSource) {
        MongoSourceDTO mongoSourceDTO = (MongoSourceDTO) iSource;
        boolean check = false;
        MongoClient mongoClient = null;
        try {
            mongoClient = getClient(mongoSourceDTO);
            String schema = StringUtils.isBlank(mongoSourceDTO.getSchema()) ? dealSchema(mongoSourceDTO.getHostPort()) : mongoSourceDTO.getSchema();
            MongoDatabase mongoDatabase = mongoClient.getDatabase(schema);
            MongoIterable<String> mongoIterable = mongoDatabase.listCollectionNames();
            mongoIterable.iterator().hasNext();
            check = true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (!isOpenPool.get() && mongoClient != null) {
                mongoClient.close();
            }
        }
        return check;
    }

    //获取数据库
    public static List<String> getDatabaseList(ISourceDTO iSource){
        MongoSourceDTO mongoSourceDTO = (MongoSourceDTO) iSource;
        MongoClient mongoClient = null;
        ArrayList<String> databases = new ArrayList<>();
        try {
            mongoClient = getClient(mongoSourceDTO);
            MongoIterable<String> dbNames = mongoClient.listDatabaseNames();
            for (String dbName:dbNames){
                databases.add(dbName);
            }
        }catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (!isOpenPool.get() && mongoClient != null) {
                mongoClient.close();
            }
        }
        return databases;
    }

    /**
     * 预览数据
     */
    public static List<List<Object>> getPreview(ISourceDTO iSource, SqlQueryDTO queryDTO){
        MongoSourceDTO mongoSourceDTO = (MongoSourceDTO) iSource;
        List<List<Object>> dataList = new ArrayList<>();
        String schema = StringUtils.isBlank(mongoSourceDTO.getSchema()) ? dealSchema(mongoSourceDTO.getHostPort()) : mongoSourceDTO.getSchema();
        if (StringUtils.isBlank(queryDTO.getTableName()) || StringUtils.isBlank(schema)) {
            return dataList;
        }
        MongoClient mongoClient = null;
        try {
            mongoClient = getClient(mongoSourceDTO);
            //获取指定数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(schema);
            //获取指定表
            MongoCollection<Document> collection = mongoDatabase.getCollection(queryDTO.getTableName());
            FindIterable<Document> documents = collection.find().limit(queryDTO.getPreviewNum());
            for (Document document:documents){
                ArrayList<Object> list = new ArrayList<>();
                document.keySet().forEach(key->list.add(new Pair<String, Object>(key, document.get(key))));
                dataList.add(list);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (!isOpenPool.get() && mongoClient != null) {
                mongoClient.close();
            }
        }
        return dataList;
    }

    //获取指定库下的表名
    public static List<String> getTableList(ISourceDTO iSource) {
        MongoSourceDTO mongoSourceDTO = (MongoSourceDTO) iSource;
        List<String> tableList = Lists.newArrayList();
        MongoClient mongoClient = null;
        try {
            String db = StringUtils.isBlank(mongoSourceDTO.getSchema()) ? dealSchema(mongoSourceDTO.getHostPort()) : mongoSourceDTO.getSchema();
            mongoClient = getClient(mongoSourceDTO);
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoIterable<String> tableNames = mongoDatabase.listCollectionNames();
            for (String s : tableNames) {
                tableList.add(s);
            }
        } catch (Exception e) {
            log.error("获取tablelist异常  {}", mongoSourceDTO, e);
        } finally {
            if (!isOpenPool.get() && mongoClient != null) {
                mongoClient.close();
            }
        }
        return tableList;
    }

    public static List<ServerAddress> getServerAddress(String hostPorts) {
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


    public static MongoClient getClient(MongoSourceDTO mongoSourceDTO) {
        String hostPorts = mongoSourceDTO.getHostPort();
        String username = mongoSourceDTO.getUsername();
        String password = mongoSourceDTO.getPassword();
        String schema = mongoSourceDTO.getSchema();
        boolean check = false;
        //适配之前的版本，判断ISourceDTO类中有无获取isCache字段的方法
        Field[] fields = MongoSourceDTO.class.getDeclaredFields();
        for (Field field : fields) {
            if (poolConfigFieldName.equals(field.getName())) {
                check = mongoSourceDTO.getPoolConfig() != null;
                break;
            }
        }
        isOpenPool.set(check);
        //不开启连接池
        if (!check) {
            return getClient(hostPorts, username, password, schema);
        }
        //开启连接池
        return mongoManager.getConnection(mongoSourceDTO);
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
            uri.append(String.format("mongodb://%s:%s@%s", URLEncoder.encode(usernameUrl),
                    URLEncoder.encode(passwordUrl), elseUrl));
            clientURI = new MongoClientURI(uri.toString());
            mongoClient = new MongoClient(clientURI);
        } else {
            List<ServerAddress> serverAddress = getServerAddress(hostPorts);
            if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
                mongoClient = new MongoClient(serverAddress, options);
            } else {
                if (StringUtils.isBlank(db)){
                    db = dealSchema(hostPorts);
                }
                MongoCredential credential = MongoCredential.createScramSha1Credential(username, db,
                        password.toCharArray());
                List<MongoCredential> credentials = Lists.newArrayList();
                credentials.add(credential);
                mongoClient = new MongoClient(serverAddress, credentials, options);
            }
        }
        return mongoClient;
    }

    /**
     * 如果没有指定schema，判断hostPort中有没有
     * @param hostPorts
     * @return
     */
    public static String dealSchema(String hostPorts) {
        for (String hostPort : hostPorts.split(HOST_SPLIT_REGEX)) {
            if (hostPort.length() == 0) {
                continue;
            }
            Matcher matcher = HOST_PORT_SCHEMA_PATTERN.matcher(hostPort);
            if (matcher.find()) {
                String schema = matcher.group("schema");
                return schema;
            }
        }
        return null;
    }
}
