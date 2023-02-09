package com.realworldproject.producer;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;


public class TwitterProducer {

    public static void main(String[] args) throws ApiException {

        String bearer_token ="YOUR BEARER TOKEN HERE";
        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer(bearer_token));
        KafkaProducer<String,String> producer = createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("SHutdown hook");
            apiInstance.setApiClient(null);
            producer.close();
            System.out.println("Application has exited");
        }
        ));


        Set<String> tweetFields = new HashSet<>();
        
        tweetFields.add("author_id");
        tweetFields.add("id");
        tweetFields.add("created_at");
        tweetFields.add("text");
    
        String query = "bitcoin";

        Get2TweetsSearchRecentResponse result = apiInstance.tweets().tweetsRecentSearch(query).tweetFields(tweetFields).execute();

        if(result.getErrors() != null && result.getErrors().size() > 0) {
            System.out.println("Error:");
            result.getErrors().forEach(e -> {
                System.out.println(e.toString());
                if (e instanceof ResourceUnauthorizedProblem) {
                System.out.println(((ResourceUnauthorizedProblem) e).getTitle() + " " + ((ResourceUnauthorizedProblem) e).getDetail());
                }
            });
            } else {
                if (result.getData() != null) {
                    
                    for (Tweet tweet : result.getData()) {
                        System.out.println(tweet.toJson());
                        producer.send(new ProducerRecord<String,String>("twitter-tweets2",tweet.toJson()), new Callback() {
                            public void onCompletion(RecordMetadata recordMetaData, Exception e){
                                if(e != null){ 
                                    System.out.println("Something bad happened");
                                }
                            }
                        });
                        // System.out.println("Id : " + tweet.getId());
                        // System.out.println("Text : " + tweet.getText());
                        // System.out.println("Author : " + tweet.getAuthorId());
                        // System.out.println("Created at :" +tweet.getCreatedAt()+"\n");
                    }
        
                }
            }  
            
    }
    public static KafkaProducer<String,String> createKafkaProducer(){
        String bootstrap_server = "localhost:9092";

        Properties properties = new Properties();
    
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe prodcuer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // we are using kafka 2.0>=1.1 s0
        // use 5 else use 1

        //high throughput at expense of latency and CPU usage
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        //2. Create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        return producer;
    }

}