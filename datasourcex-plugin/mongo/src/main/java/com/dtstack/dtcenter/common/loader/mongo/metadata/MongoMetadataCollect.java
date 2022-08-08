/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.common.loader.mongo.metadata;

import com.dtstack.dtcenter.common.loader.metadata.collect.AbstractMetaDataCollect;
import com.dtstack.dtcenter.common.loader.mongo.MongoDBUtils;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.mongo.MetadataMongoEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.mongo.MongoCollectionEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.mongo.MongoColumnEntity;
import com.dtstack.dtcenter.loader.dto.source.MongoSourceDTO;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Aggregates;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static com.dtstack.dtcenter.common.loader.mongo.metadata.constants.MongoCons.KEY_$;
import static com.dtstack.dtcenter.common.loader.mongo.metadata.constants.MongoCons.KEY_$TYPE;
import static com.dtstack.dtcenter.common.loader.mongo.metadata.constants.MongoCons.KEY_COLLSTATS;
import static com.dtstack.dtcenter.common.loader.mongo.metadata.constants.MongoCons.KEY_COUNT;
import static com.dtstack.dtcenter.common.loader.mongo.metadata.constants.MongoCons.KEY_DATATYPE;
import static com.dtstack.dtcenter.common.loader.mongo.metadata.constants.MongoCons.KEY_SIZE;
import static com.dtstack.dtcenter.common.loader.mongo.metadata.constants.MongoCons.KEY_VALIDATE;

/**
 * mongo 元数据采集器
 *
 * @author by zhiyi
 * @date 2022/4/11 10:04 上午
 */
@Slf4j
public class MongoMetadataCollect extends AbstractMetaDataCollect {

    private transient MongoClient client;

    private transient MongoDatabase database;

    @Override
    protected void openConnection() {
        MongoSourceDTO mongoSourceDTO = (MongoSourceDTO) sourceDTO;
        try {
            if (client == null) {
                client = MongoDBUtils.getClient(mongoSourceDTO);
            }
            database = client.getDatabase(currentDatabase);
            if (CollectionUtils.isEmpty(tableList)) {
                tableList = showTables();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected MetadataEntity createMetadataEntity() {
        // Run validate on each collection on the mongod to
        // restore the correct statistics after an unclean shutdown.
        database.runCommand(new Document(KEY_VALIDATE, currentObject));
        Document document = database.runCommand(new Document(KEY_COLLSTATS, currentObject));
        MetadataMongoEntity entity = new MetadataMongoEntity();

        entity.setTableName((String) currentObject);
        entity.setSchema(currentDatabase);

        MongoCollectionEntity collectionEntity = new MongoCollectionEntity();
        collectionEntity.setCount((int) document.get(KEY_COUNT));
        collectionEntity.setTotalSize((int) document.get(KEY_SIZE));
        entity.setTableProperties(collectionEntity);

        MongoCollection<Document> collection = database.getCollection((String) currentObject);
        List<MongoColumnEntity> columnEntities = createColumnEntities(collection);
        entity.setColumns(columnEntities);
        return entity;
    }

    private List<MongoColumnEntity> createColumnEntities(MongoCollection<Document> collection) {
        List<MongoColumnEntity> columnEntities = new ArrayList<>();
        MongoCursor<Document> cursor = collection.find()
                .limit(1)
                .cursor();
        int index = 0;
        if (cursor.hasNext()) {
            for (String name : cursor.next().keySet()) {
                MongoColumnEntity columnEntity = new MongoColumnEntity();
                columnEntity.setIndex(++index);
                columnEntity.setName(name);
                collection.aggregate(Arrays.asList(
                        Aggregates.limit(1),
                        Aggregates.project(new Document(KEY_DATATYPE, new Document(KEY_$TYPE, KEY_$ + name)))))
                        .forEach((Consumer<Document>) document -> columnEntity.setType((String) document.get(KEY_DATATYPE)));
                columnEntities.add(columnEntity);
            }
        }
        //cursor need to close.
        cursor.close();
        return columnEntities;
    }

    @Override
    protected List<Object> showTables() {
        MongoIterable<String> collectionNames = database.listCollectionNames();
        List<Object> collectionList = new ArrayList<>();

        collectionNames.forEach((Consumer<String>) collectionList::add);
        return collectionList;
    }

    @Override
    public void close() {
        if (client != null) {
            log.info("Start close mongodb client.");
            client.close();
            client = null;
            log.info("Close mongodb client successfully.");
        }
        database = null;
    }
}
