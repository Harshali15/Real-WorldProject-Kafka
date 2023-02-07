package com.realworldproject.kafka;

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
    
        String query = "kafka";

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
                        producer.send(new ProducerRecord<String,String>("twitter-tweets",tweet.getText()), new Callback() {
                            public void onCompletion(RecordMetadata recordMetaData, Exception e){
                                if(e != null){ 
                                    System.out.println("Something bad happened");
                                }
                            }
                        });
                        System.out.println("Id : " + tweet.getId());
                        System.out.println("Text : " + tweet.getText());
                        System.out.println("Author : " + tweet.getAuthorId());
                        System.out.println("Created at :" +tweet.getCreatedAt()+"\n");
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

        //2. Create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        return producer;
    }

}