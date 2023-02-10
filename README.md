# kafka-real-world-project
*** Real world twitter producer consumer using kafka ***

This project is about creating a producer-consumer architecture using kafka to consume tweets from twitter in real time and sending them to elasticsearch. The project flow diagram is as below : 

|-------------|      |--------------|      |---------------|      |--------------|
| Twitter API |----\ | Kafka        |----\ | Kafka         |----\ |ElasticSearch |
|             |----/ | Producer     |----/ | Consumer      |----/ |              |
|-------------|      |--------------|      |---------------|      |--------------|

I have worked step by step, first coding just the producer, then a consumer, and then added improvements to them for better throughput. In the end I have tried a simple hands-on implementations on kafka connect and kafka streams. 

 Description of classes and folders:
 1. TwitterApiExample.java
 **This class impelements a basic code to fetch data from twitter api, fetches tweets based on a query.**

2. TwitterProducer.java
**This class creates a producer to send data fetched form twitter api to a topicin kafka**

3. ElasticSearchConsumer.java
**This class creates a consumer to consume the produced data from TwitterProducer.java and sends the data to elasticsearch cluster created using bonsai elasticsearch**

4. ConsumerImrpved.java
**This class creates a consumer with improved performance by batching the messages and consuming in bulk using BulkRequest class**

5. StreamFilterTweets.java
**This class is used to try an implementation of Kafka Streams and use it to filter out the tweets to consume**

6. connectors
**This folder contains kafka-connect-twitter source zip file extracted, and two properties file to run the same Your have to modify 'the plugin.path' in your connect-standalone.properties found inside config foler under kafka**
**Configure your twitter credentials in the twitter.properties file**

*Note: All the following codes are implemented referring to Kafka-Beginners-Course by Stephane Mareek*