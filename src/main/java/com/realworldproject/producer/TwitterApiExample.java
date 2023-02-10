package com.realworldproject.producer;

import java.util.HashSet;
import java.util.Set;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;


public class TwitterApiExample {

    public static void main(String[] args) throws ApiException {

        String bearer_token ="YOUR BEARER TOKEN HERE";
        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer(bearer_token));

            
        Set<String> tweetFields = new HashSet<>();
        
        tweetFields.add("author_id");
        tweetFields.add("id");
        tweetFields.add("created_at");
    

        String query = "bitcoin";

        Get2TweetsSearchRecentResponse result = apiInstance.tweets().tweetsRecentSearch(query).execute();

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
                            System.out.println("Id : " + tweet.getId());
                            System.out.println("Text : " + tweet.getText());
                            System.out.println("Author : " + tweet.getAuthorId());
                            System.out.println("Created at :" +tweet.getCreatedAt()+"\n");
                        }
            
                    }
                }
        
    }

    

}