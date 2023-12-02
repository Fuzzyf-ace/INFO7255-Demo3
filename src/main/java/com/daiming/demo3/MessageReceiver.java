package com.daiming.demo3;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import jakarta.json.Json;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.index.Settings;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class MessageReceiver {

    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = "planQueue")
    public void receiveMessage(String message) {
//        System.out.println(message);
        //check if the index exists
        if (!elasticsearchOperations.indexOps(IndexCoordinates.of("plan_index")).exists()) {
            // Create the index with mapping
           IndexOperations indexOperations =  elasticsearchOperations.indexOps(IndexCoordinates.of("plan_index"));
            indexOperations.create();
            String mapping = "{\"properties\":{\"planType\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"deductible\":{\"type\":\"long\"},\"name\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"_org\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"copay\":{\"type\":\"long\"},\"creationDate\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"plan_join\":{\"type\":\"join\",\"relations\":{\"linkedPlanServices\":[\"linkedService\",\"planserviceCostShares\"],\"plan\":[\"planCostShares\",\"linkedPlanServices\"]},\"eager_global_ordinals\":true},\"objectId\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"objectType\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}},\"_routing\":{\"required\":false}}";
            Document document = Document.parse(mapping);
            indexOperations.putMapping(document);
        }


        Map<String, Object> messageMap = null;
        try {
            messageMap = objectMapper.readValue(message, HashMap.class);
            String operation = (String) messageMap.get("operation");
            Map<String, Object> object = (Map<String, Object>) messageMap.get("object");
            if (operation.equals("create")) {
                indexDocument(object);
            } else if (operation.equals("delete")) {
                deleteDocument("plan_index", (String) object.get("objectId"));
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }



    private boolean deleteDocument(String indexName, String documentId) {
        try {
//            elasticsearchOperations.delete
            elasticsearchOperations.delete(documentId, IndexCoordinates.of(indexName));
            return true;  // Deletion successful
        } catch (DataAccessException e) {
            // Log the exception or handle it based on your needs
            e.printStackTrace();
            return false; // Deletion failed
        }
    }

    private boolean indexDocument(Map<String, Object> object) {
        try {
            String id = (String) object.get("objectId");
            HashMap<String, Object> planJoin = (HashMap<String, Object>) object.get("plan_join");
            String routing = (String) planJoin.get("parent");

            IndexQuery indexQuery = new IndexQueryBuilder()
                    .withId(id)
                    .withIndex("plan_index")
                    .withRouting(routing)
                    .withObject(object)
                    .build();
            elasticsearchOperations.index(indexQuery, IndexCoordinates.of("plan_index"));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


}