package com.realworldproject.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;


public class ConsumerImproved {
    public static RestHighLevelClient createClient(){

        //I am using elasticsearch cluster from bonsaisearch.net. Create you cluster and then add these detaisl here
        String hostname = "hostname";
        String username = "username";
        String password = "password";

        final CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

            RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                @Override
                                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder){
                                    return httpClientBuilder.setDefaultCredentialsProvider(cp);
                                }
                            });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;

    }

    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrap_server = "localhost:9092";
        String groupId = "kafka-project-elasticsearch";
        
        //Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        //1. create consumer configs/properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest/latest/none
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//disable auto commit
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); //poll a max of 100 records

        //2. create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ConsumerImproved.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String,String> consumer = createConsumer("twitter-tweets2");
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            
            Integer record_count= records.count();

            logger.info("Received "+ record_count+ " records");

            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String,String> record : records) {

                //one way to provide id , kafka generic way
                //String id = record.topic()+"_"+ record.partition()+"_"+record.offset();

                //other way to provide id, twitter feed specifc id
                try{
                    String id1 = extractIdFromTweet(record.value());
                IndexRequest indexRequest = new IndexRequest(
                    "twitter",
                    "tweets",
                    id1   //this will make our consumer idempotent
                ).source(record.value(),XContentType.JSON);

                bulkRequest.add(indexRequest);  //we add to out bulk request
                }catch(NullPointerException e){
                    logger.warn("Skipping bad data "+ record.value());
                }
                
            }   
            if(record_count>0){
                BulkResponse bulkItemResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Commiting offsets now");
                consumer.commitSync();
                logger.info("Offsets commited");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        //client.close();
    }

    private static String extractIdFromTweet(String tweetJson) {

        return JsonParser.parseString(tweetJson)
        .getAsJsonObject()
        .get("id")
        .getAsString();
    }
}
