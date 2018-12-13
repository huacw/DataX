package com.alibaba.datax.plugin.writer.estransportwriter;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ESClient {
    private static final Logger log = LoggerFactory.getLogger(ESClient.class);

    private TransportClient client;

    public TransportClient getClient() {
        return client;
    }

    public void createClient(String endpoint,
                             String user,
                             String passwd,
                             String clusterName) {

        Map<String, Object> otherSetting = new HashMap<>();
        if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(passwd)) {
            otherSetting.put("http.basic.user", user);
            otherSetting.put("http.basic.password", passwd);
        }
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true)
                .put(otherSetting)
                .build();

        client = TransportClient.builder().settings(settings).build();

        try {
            for (String address : endpoint.split(",")) {
                String[] add = address.split(":");
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(add[0]), Integer.parseInt(add[1])));
            }
        } catch (UnknownHostException e) {
            log.error("集群地址配置异常", e);
            throw new DataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, String.format("异常信息：%s", e.getMessage()));
        }
    }

    public boolean indicesExists(String indexName) throws Exception {
        return client.admin().indices().exists(new IndicesExistsRequest().indices(new String[]{indexName})).get(5, TimeUnit.MINUTES).isExists();
    }

    public boolean deleteIndex(String indexName) throws Exception {
        log.info("delete index " + indexName);
        if (indicesExists(indexName)) {
            client.admin().indices().prepareDelete(indexName).get();
        } else {
            log.info("index cannot found, skip delete " + indexName);
        }
        return true;
    }

    public boolean createIndex(String indexName, String typeName,
                               String mappings, String settings, boolean dynamic) {
        try {
            if (!indicesExists(indexName)) {
                log.info("create index " + indexName);
                //创建索引
                client.admin().indices().prepareCreate(indexName).setSettings(settings).execute(new ActionListener<CreateIndexResponse>() {
                    @Override
                    public void onResponse(CreateIndexResponse createIndexResponse) {
                        log.info(String.format("create [%s] index success", indexName));
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        log.error("创建索引失败", e);
                        log.error(String.format("create [%s] index fail,err msg : %s", indexName, e.getMessage()));
                    }
                });
            }

            int idx = 0;
            while (idx < 5) {
                if (indicesExists(indexName)) {
                    break;
                }
                Thread.sleep(2000);
                idx++;
            }
            if (idx >= 5) {
                return false;
            }

            if (dynamic) {
                log.info("ignore mappings");
                return true;
            }
            log.info("create mappings for " + indexName + "  " + mappings);
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexName).type(typeName).source(mappings);
            client.admin().indices().putMapping(mappingRequest, new ActionListener<PutMappingResponse>() {
                @Override
                public void onResponse(PutMappingResponse putMappingResponse) {
                    log.info(String.format("create mapping of [%s] index success", indexName));
                }

                @Override
                public void onFailure(Throwable e) {
                    log.error("创建mapping失败", e);
                    log.error(String.format("create mapping of [%s]  fail,err msg : %s", indexName, e.getMessage()));
                }
            });
            return true;
        } catch (Exception e) {
            log.error("创建索引异常", e);
            return false;
        }
    }

//    public boolean isBulkResult(JestResult rst) {
//        JsonObject jsonObject = rst.getJsonObject();
//        return jsonObject.has("items");
//    }

//    public JestResult bulkInsert(Bulk.Builder bulk, int trySize) throws Exception {
//        // es_rejected_execution_exception
//        // illegal_argument_exception
//        // cluster_block_exception
//        JestResult rst = null;
//        rst = jestClient.execute(bulk.build());
//        if (!rst.isSucceeded()) {
//            log.warn(rst.getErrorMessage());
//        }
//        return rst;
//    }

    /**
     * ES记录类
     */
    public static class ESRecord {
        private String id;
        private String jsonData;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getJsonData() {
            return jsonData;
        }

        public void setJsonData(String jsonData) {
            this.jsonData = jsonData;
        }
    }

    public BulkItemResponse[] insert(String indexName, String typeName, List<ESRecord> datas) {
        try {
            if (datas == null || datas.isEmpty()) {
                log.warn("没有要插入es的数据");
                return null;
            }
            BulkRequest bulkRequest = Requests.bulkRequest();
            for (ESRecord data : datas) {
                IndexRequest indexRequest = Requests.indexRequest().index(indexName).type(typeName);
                if (StringUtils.isNotBlank(data.getId())) {
                    indexRequest.id(data.getId());
                }
                bulkRequest.add(indexRequest.source(data.getJsonData()));
            }
            return client.bulk(bulkRequest).actionGet().getItems();
        } catch (Exception e) {
            log.error("插入es异常", e);
            return null;
        }
    }

    /**
     * 关闭Client客户端
     */
    public void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        ESClient client = new ESClient();
        client.createClient("192.168.55.136:9300,192.168.55.137:9300,192.168.55.138:9300", null, null, "cmc-qa-es");
//        client.createIndex("test", "test", null, "", true);
//        client.deleteIndex("test");
        List<ESRecord> datas = new ArrayList<>();
        ESRecord record = new ESRecord();
        record.setJsonData("{\"id\":1,\"name\":\"Jack\",\"age\":23}");
        record.setId("2");
//        datas.add("{\"id\":1,\"name\":\"Jack\",\"age\":23}");
//        datas.add("{\"id\":2,\"name\":\"Jack1\",\"age\":30}");
//        datas.add("{\"id\":3,\"name\":\"Jack3\",\"age\":32}");
        datas.add(record);
        client.insert("test", "test", datas);
        client.closeClient();
    }
}
